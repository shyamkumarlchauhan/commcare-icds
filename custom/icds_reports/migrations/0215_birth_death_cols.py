# Generated by Django 2.2.13 on 2020-10-01 15:21

from django.db import migrations, models
import custom.icds_reports.models.aggregate



class Migration(migrations.Migration):

    dependencies = [
        ('icds_reports', '0214_aggregatepersoncase'),
    ]

    operations = [
        migrations.AddField(
            model_name='aggmprawc',
            name='delivery_death_permanent_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='delivery_death_temp_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='mother_death_permanent_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='mother_death_temp_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='pnc_death_permanent_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='pnc_death_temp_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='pregnancy_death_permanent_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggmprawc',
            name='pregnancy_death_temp_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='AggregatePersonCase',
            name='delivery_death_permanent_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggregatepersoncase',
            name='delivery_death_temp_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggregatepersoncase',
            name='mother_death_permanent_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggregatepersoncase',
            name='mother_death_temp_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggregatepersoncase',
            name='pnc_death_permanent_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggregatepersoncase',
            name='pnc_death_temp_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggregatepersoncase',
            name='pregnancy_death_permanent_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='aggregatepersoncase',
            name='pregnancy_death_temp_resident',
            field=models.IntegerField(null=True),
        ),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN weighed_within_3_days smallint"),
        migrations.RunSQL("ALTER TABLE child_health_monthly ADD COLUMN mother_resident_status TEXT"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN still_birth_permanent_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN still_birth_temp_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN weighed_in_3_days_permanent_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN weighed_in_3_days_temp_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN neonatal_deaths_permanent_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN neonatal_deaths_temp_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN post_neonatal_deaths_permanent_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN post_neonatal_deaths_temp_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN total_deaths_permanent_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN total_deaths_temp_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN lbw_permanent_resident INTEGER"),
        migrations.RunSQL("ALTER TABLE agg_child_health ADD COLUMN lbw_temp_resident INTEGER"),
    ]
