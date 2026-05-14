"""
Compatibility re-exports.

The upload pipeline implementation has been split into focused modules:
- apps.upload.amazon
- apps.upload.flipkart
- apps.upload.dashboard_builders
"""

from apps.upload.amazon import (
    process_category_file,
    process_fba_stock_file,
    process_flex_stock_file,
    process_price_file,
    process_sales_file,
    process_spend_file,
)
from apps.upload.dashboard_builders import (
    generate_dashboard_data,
    generate_flipkart_dashboard_data,
)
from apps.upload.flipkart import (
    process_fk_category,
    process_fk_fba_stock_file,
    process_fk_flex_stock_file,
    process_fk_inventory_file,
    process_fk_pla,
    process_fk_price,
    process_fk_search_traffic,
)

__all__ = [
    "process_category_file",
    "process_fba_stock_file",
    "process_flex_stock_file",
    "process_price_file",
    "process_sales_file",
    "process_spend_file",
    "generate_dashboard_data",
    "generate_flipkart_dashboard_data",
    "process_fk_category",
    "process_fk_fba_stock_file",
    "process_fk_flex_stock_file",
    "process_fk_inventory_file",
    "process_fk_pla",
    "process_fk_price",
    "process_fk_search_traffic",
]
