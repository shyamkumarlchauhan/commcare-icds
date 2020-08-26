# Generated by Django 2.2.13 on 2020-08-23 09:28

import custom.icds.models
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('icds', '0007_auto_20200226_0912'),
    ]

    operations = [
        migrations.CreateModel(
            name='VaultEntry',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('key', models.CharField(default=custom.icds.models._default_key, max_length=25)),
                ('value', models.CharField(max_length=255)),
                ('date_created', models.DateTimeField(auto_now_add=True)),
                ('form_id', models.CharField(db_index=True, default=None, max_length=255)),
            ],
        ),
        migrations.AddIndex(
            model_name='vaultentry',
            index=models.Index(fields=['key'], name='icds_vaulte_key_f8dec5_idx'),
        ),
    ]
