
from .base import BaseSender, CrashInfoProcessed

import subprocess
from email.mime.text import MIMEText


class EmailSender(BaseSender):
    def __init__(self, logger, emails):
        super().__init__()
        self._logger = logger
        self._emails = emails

    def send(self, crash: CrashInfoProcessed):
        self._logger.info("Send core to email %r", self._emails)
        mail_from = "devnull@example.com"
        mail_to = ", ".join(self._emails)
        message_body = [crash.get_header()]
        if crash.core_url:
            message_body.append("URL: " + crash.core_url)
        if crash.info:
            message_body.append("")
            message_body.append(crash.info)
        if crash.crash_type != CrashInfoProcessed.CRASH_TYPE_OOM:
            message_body.append("")
            message_body.append(crash.formatted_backtrace)

        message = MIMEText("\n".join(message_body))
        message["Subject"] = \
            "[{ctype}] {crash_type} {service} on {server}".format(
                ctype=crash.cluster,
                crash_type=crash.crash_type,
                service=crash.service_name,
                server=crash.server,
            )
        message["From"] = "{server} <{address}>".format(
            server=crash.server,
            address=mail_from
        )
        message["To"] = mail_to

        try:
            sendmail = subprocess.Popen(
                ["/usr/sbin/sendmail", "-t"], stdin=subprocess.PIPE)
            sendmail.communicate(message.as_string().encode("utf-8"))
            sendmail.wait()
        except Exception as e:
            self._logger.error("sendmail error %r", e)
            self._logger.debug("Exception", exc_info=True)
