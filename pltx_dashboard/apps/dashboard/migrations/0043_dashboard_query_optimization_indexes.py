from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dashboard", "0042_dashboard_product_daily_summary"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="processeddashboarddata",
            index=models.Index(
                fields=["user", "total_spend"], name="idx_pdd_u_spend"
            ),
        ),
        migrations.AddIndex(
            model_name="flipkartprocesseddashboarddata",
            index=models.Index(
                fields=["user", "total_spend"], name="idx_fkpdd_u_spend"
            ),
        ),
        migrations.AddIndex(
            model_name="flipkartsearchtraffic",
            index=models.Index(fields=["fsn", "date"], name="idx_fkst_fsn_date"),
        ),
    ]
