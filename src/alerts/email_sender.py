"""Outlook SMTP email sender with HTML templates."""
from __future__ import annotations
import smtplib, ssl, logging
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from src.utils.config import get_settings

log = logging.getLogger(__name__)

_SEV_COLOR = {"critical": "#ef4444", "warning": "#f59e0b", "info": "#22c55e"}
_ISSUE_ICON = {"broken":"💥","delayed":"⏰","paused":"⏸","rescheduled":"🔄",
               "drop":"📉","spike":"📈","zero":"🚫","ml_anomaly":"🤖"}

def _badge(text: str, color: str) -> str:
    return (f'<span style="background:{color};color:#fff;padding:2px 8px;'
            f'border-radius:9999px;font-size:11px;font-weight:700;">{text.upper()}</span>')

def _html_alert(alert) -> str:
    color = _SEV_COLOR.get(alert.severity, "#6b7280")
    icon  = _ISSUE_ICON.get(alert.issue, "⚠️")
    rows  = "".join(
        f'<tr><td style="padding:8px 12px;color:#6b7280;white-space:nowrap;">{k}</td>'
        f'<td style="padding:8px 12px;font-weight:600;">{v}</td></tr>'
        for k, v in {
            "Connector":  f"{alert.connector_name} ({alert.connector_id})",
            "Issue":      _badge(alert.issue, color),
            "Severity":   _badge(alert.severity, color),
            "Detail":     alert.reason,
            "Alert Type": alert.alert_type.replace("_"," ").title(),
            "Time":       alert.fired_at,
        }.items()
    )
    return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f3f4f6;">
<table width="600" style="margin:20px auto;background:#fff;border-radius:8px;
       overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.1);">
  <tr><td style="background:{color};height:6px;"></td></tr>
  <tr><td style="padding:24px 28px;background:{color}11;">
    <p style="margin:0;font-size:11px;font-weight:700;color:{color};text-transform:uppercase;">
      Fivetran Monitor</p>
    <h2 style="margin:6px 0 0;font-size:18px;">{icon} {alert.issue.upper()} — {alert.connector_name}</h2>
  </td></tr>
  <tr><td style="padding:20px 28px;">
    <table width="100%" style="border:1px solid #e5e7eb;border-radius:6px;">{rows}</table>
  </td></tr>
  <tr><td style="padding:12px 28px;background:#f9fafb;font-size:11px;color:#9ca3af;">
    Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} · Fivetran Monitor
  </td></tr>
</table></body></html>"""

def _html_summary(rule_alerts: list, ml_alerts: list) -> str:
    total   = len(rule_alerts) + len(ml_alerts)
    healthy = total == 0
    color   = "#22c55e" if healthy else "#f59e0b"
    if any(getattr(a,"severity","") == "critical" for a in rule_alerts):
        color = "#ef4444"
    header  = "✅ All connectors healthy" if healthy else f"⚠️ {total} issue(s) detected"
    rows_html = ""
    for a in rule_alerts:
        c = _SEV_COLOR.get(getattr(a,"severity","warning"), "#f59e0b")
        rows_html += (
            f'<tr><td style="padding:8px 12px;">{getattr(a,"connector_name","")}</td>'
            f'<td style="padding:8px 12px;">{_badge(getattr(a,"issue",""), c)}</td>'
            f'<td style="padding:8px 12px;">{_badge(getattr(a,"severity",""), c)}</td>'
            f'<td style="padding:8px 12px;font-size:12px;color:#6b7280;">{getattr(a,"reason","")}</td></tr>'
        )
    table = f"""<table width="100%" style="border:1px solid #e5e7eb;border-radius:6px;margin-top:16px;">
      <tr style="background:#f9fafb;">
        <th style="padding:8px 12px;text-align:left;font-size:11px;color:#6b7280;">Connector</th>
        <th style="padding:8px 12px;text-align:left;font-size:11px;color:#6b7280;">Issue</th>
        <th style="padding:8px 12px;text-align:left;font-size:11px;color:#6b7280;">Severity</th>
        <th style="padding:8px 12px;text-align:left;font-size:11px;color:#6b7280;">Detail</th>
      </tr>{rows_html}</table>""" if rows_html else ""
    return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;background:#f3f4f6;">
<table width="600" style="margin:20px auto;background:#fff;border-radius:8px;overflow:hidden;">
  <tr><td style="background:{color};height:6px;"></td></tr>
  <tr><td style="padding:24px 28px;background:{color}11;">
    <h2 style="margin:0;">{header}</h2></td></tr>
  <tr><td style="padding:20px 28px;">
    <p>Checked all connectors at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}.</p>
    <p>Rule alerts: <strong>{len(rule_alerts)}</strong> &nbsp;|&nbsp;
       ML alerts: <strong>{len(ml_alerts)}</strong></p>
    {table}
  </td></tr>
</table></body></html>"""

def _smtp_send(subject: str, html: str, plain: str = "") -> bool:
    cfg = get_settings().email
    if not cfg.enabled: return False
    sender = cfg.smtp_username if cfg.is_outlook else cfg.from_addr
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(cfg.to_addrs)
    if plain: msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))
    try:
        if cfg.use_tls:
            domain = cfg.smtp_username.split("@")[-1] if cfg.smtp_username else "localhost"
            with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=30) as s:
                s.ehlo(domain); s.starttls(context=ssl.create_default_context()); s.ehlo(domain)
                s.login(cfg.smtp_username, cfg.smtp_password)
                s.sendmail(sender, cfg.to_addrs, msg.as_bytes())
        else:
            with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port,
                                   context=ssl.create_default_context(), timeout=30) as s:
                s.login(cfg.smtp_username, cfg.smtp_password)
                s.sendmail(sender, cfg.to_addrs, msg.as_bytes())
        log.info("Email sent: %s → %s", subject, cfg.to_addrs)
        return True
    except smtplib.SMTPAuthenticationError:
        log.error("SMTP auth failed — check credentials / app password for Office 365")
    except smtplib.SMTPException as exc:
        log.error("SMTP error: %s", exc)
    except OSError as exc:
        log.error("SMTP connection failed: %s", exc)
    return False

def send_email_alert(alert) -> bool:
    sev_label = {"critical":"[CRITICAL]","warning":"[WARNING]"}.get(
        getattr(alert,"severity","warning"), "[ALERT]")
    subject = f"[Fivetran] {sev_label} {getattr(alert,'issue','').upper()}: {getattr(alert,'connector_name','')}"
    return _smtp_send(subject, _html_alert(alert), getattr(alert,"reason",""))

def send_summary_email(rule_alerts: list, ml_alerts: list) -> bool:
    total   = len(rule_alerts) + len(ml_alerts)
    subject = ("[Fivetran] ✅ All connectors healthy" if total == 0
               else f"[Fivetran] ⚠️ {total} issue(s) — action required")
    return _smtp_send(subject, _html_summary(rule_alerts, ml_alerts))
