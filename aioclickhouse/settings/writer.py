import logging

from aioclickhouse.writer import write_binary_str
from aioclickhouse.settings.available import (
    settings as available_settings,
    limits as available_limits
)


def write_settings(settings, buf):
    for setting, value in (settings or {}).items():
        setting_writer = (
            available_settings.get(setting) or
            available_limits.get(setting)
        )

        if not setting_writer:
            logging.warning('Unknown setting %s. Skipping', setting)
            continue

        write_binary_str(setting, buf)
        setting_writer.write(value, buf)

    write_binary_str('', buf)  # end of settings
