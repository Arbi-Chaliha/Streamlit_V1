from django.db import models


class Failure(models.Model):
    label = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.label

class RootCause(models.Model):
    label = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.label

class Trigger(models.Model):
    label = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.label

class DataChannel(models.Model):
    label = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.label

# Relationships (if you want to store them in the DB, optional)
class FailureRootCause(models.Model):
    failure = models.ForeignKey(Failure, on_delete=models.CASCADE)
    root_cause = models.ForeignKey(RootCause, on_delete=models.CASCADE)

class RootCauseTrigger(models.Model):
    root_cause = models.ForeignKey(RootCause, on_delete=models.CASCADE)
    trigger = models.ForeignKey(Trigger, on_delete=models.CASCADE)

class TriggerDataChannel(models.Model):
    trigger = models.ForeignKey(Trigger, on_delete=models.CASCADE)
    data_channel = models.ForeignKey(DataChannel, on_delete=models.CASCADE)

# Create your models here.
