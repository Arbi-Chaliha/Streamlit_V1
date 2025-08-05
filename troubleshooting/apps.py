'''from django.apps import AppConfig


class TroubleshootingConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'troubleshooting'''
from django.apps import AppConfig

class TroubleshootingConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'troubleshooting'
    verbose_name = 'FNFM Troubleshooting'

