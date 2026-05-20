from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dashboard", "0029_remove_categorymapping_idx_cm_u_cat_and_more"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="processeddashboarddata",
            index=models.Index(
                fields=["user", "date", "asin", "revenue", "units"],
                name="idx_pdd_u_d_a_r_u",
            ),
        ),
        migrations.AddIndex(
            model_name="processeddashboarddata",
            index=models.Index(
                fields=["user", "date", "asin", "units", "pageviews"],
                name="idx_pdd_u_d_a_u_pv",
            ),
        ),
        migrations.AddIndex(
            model_name="flipkartprocesseddashboarddata",
            index=models.Index(
                fields=["user", "date", "fsn", "revenue", "units"],
                name="idx_fkpd_u_d_f_r_u",
            ),
        ),
        migrations.AddIndex(
            model_name="flipkartprocesseddashboarddata",
            index=models.Index(
                fields=["user", "date", "fsn", "units", "pageviews"],
                name="idx_fkpd_u_d_f_u_pv",
            ),
        ),
    ]
