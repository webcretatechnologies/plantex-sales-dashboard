from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dashboard", "0050_dashboard_product_date_group_indexes"),
    ]

    operations = [
        migrations.AddField(
            model_name="dashboardmaterializedsummary",
            name="section_scope",
            field=models.CharField(db_index=True, default="all", max_length=20),
        ),
        migrations.AlterUniqueTogether(
            name="dashboardmaterializedsummary",
            unique_together={
                (
                    "user",
                    "view_type",
                    "section_scope",
                    "data_version",
                    "filter_hash",
                )
            },
        ),
        migrations.AddIndex(
            model_name="dashboardmaterializedsummary",
            index=models.Index(
                fields=["user", "view_type", "section_scope", "data_version"],
                name="idx_dms_u_v_s_dv",
            ),
        ),
    ]
