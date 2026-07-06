from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dashboard", "0049_dashboard_large_dataset_indexes"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(
                fields=["user", "platform", "date", "asin"],
                name="idx_dpds_u_p_d_asn",
            ),
        ),
        migrations.AddIndex(
            model_name="dashboardproductdailysummary",
            index=models.Index(
                fields=["user", "platform", "date", "fsn"],
                name="idx_dpds_u_p_d_fsn",
            ),
        ),
    ]
