import datetime
import csv
import re
from html import escape
from io import BytesIO
from io import StringIO

import pandas as pd


IST_OFFSET = pd.Timedelta(minutes=330)
BULK_QTY_THRESHOLD = 5
HIGH_VALUE_THRESHOLD = 10000
DEFAULT_REVENUE_CAP = 3500000

QC_SECTION_HEADERS = {
    "bulk": [
        "Order ID",
        "Order Date (IST)",
        "Order Value (₹)",
        "Order Qty",
        "SKU",
        "ASIN",
        "Flag",
    ],
    "qc": ["ASIN", "SKU", "Product Name", "Qty Ordered", "Order Value (₹)"],
    "category": [
        "Portfolio",
        "Category",
        "Subcategory",
        "ASIN",
        "Orders",
        "Units",
        "Revenue",
    ],
    "hourly": ["Hour (IST)", "Order ID", "ASIN", "Quantity", "Order Value (₹)"],
    "status": ["Date", "Shipped", "Pending", "Cancelled", "Total Orders"],
}

QC_SECTION_FILENAME_PREFIXES = {
    "bulk": "high_value_orders",
    "qc": "orders_need_qc_check",
    "category": "orders_by_category",
    "hourly": "orders_by_hr",
    "status": "daily_order_status",
}

QC_EMAIL_ATTACHMENT_SECTIONS = ("bulk", "qc", "category", "hourly", "status")


def _column_key(value):
    return str(value or "").replace("\ufeff", "").strip().lower()


def _read_table(file_obj):
    name = str(getattr(file_obj, "name", "") or "").lower()
    if name.endswith((".xlsx", ".xls", ".xlsm")):
        return pd.read_excel(file_obj, dtype=str).fillna("")

    file_obj.seek(0)
    try:
        df = pd.read_csv(file_obj, sep="\t", dtype=str, keep_default_na=False).fillna("")
        if len(df.columns) > 1:
            return df
    except UnicodeDecodeError:
        file_obj.seek(0)
        df = pd.read_csv(
            file_obj, sep="\t", dtype=str, keep_default_na=False, encoding="latin1"
        ).fillna("")
        if len(df.columns) > 1:
            return df

    file_obj.seek(0)
    return pd.read_csv(file_obj, dtype=str, keep_default_na=False).fillna("")


def _as_file_list(file_or_files):
    if not file_or_files:
        return []
    if isinstance(file_or_files, (list, tuple)):
        return [file_obj for file_obj in file_or_files if file_obj]
    return [file_or_files]


def _read_combined_tables(file_or_files, label):
    frames = []
    for file_obj in _as_file_list(file_or_files):
        frames.append(_normalize_columns(_read_table(file_obj)))
    if not frames:
        raise ValueError(f"Upload at least one {label}.")
    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames, ignore_index=True).fillna("")


def _normalize_columns(df):
    renamed = {}
    seen = set()
    for col in df.columns:
        key = _column_key(col)
        if key and key not in seen:
            renamed[col] = key
            seen.add(key)
    return df.rename(columns=renamed)


def _require_columns(df, required, label):
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {', '.join(missing)}")


def _to_float(value):
    text = str(value or "").replace(",", "").replace("₹", "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _to_int(value):
    try:
        return int(float(str(value or "0").replace(",", "").strip() or 0))
    except ValueError:
        return 0


def _is_cancelled(status):
    return str(status or "").strip().lower() in {"cancelled", "canceled", "cancel"}


def _ist_datetime(raw_value):
    parsed = pd.to_datetime(raw_value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return parsed + IST_OFFSET


def _display_ist(dt):
    if dt is None:
        return ""
    return dt.strftime("%d %b %H:%M IST")


def _format_money(value):
    value = float(value or 0)
    if value >= 100000:
        return f"{value / 100000:.1f}L"
    if value >= 1000:
        return f"{value / 1000:.1f}K"
    return str(int(round(value)))


def _format_full_money(value):
    return f"Rs. {float(value or 0):,.0f}"


def _date_suffix(value):
    text = str(value or "").strip()
    try:
        return datetime.date.fromisoformat(text).strftime("%d_%m_%Y")
    except ValueError:
        return _slug(text or "selected_date")


def _slug(value):
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_") or "all_categories"


def qc_report_filename(section, selected_date, extension, selected_category=None):
    section = str(section or "").strip().lower()
    prefix = QC_SECTION_FILENAME_PREFIXES.get(section, "qc_automation")
    suffix = _date_suffix(selected_date)
    ext = str(extension or "csv").strip().lower().lstrip(".")
    if ext == "excel":
        ext = "xlsx"
    if section == "status":
        category_value = "all_categories" if selected_category in {None, "", "__all__"} else selected_category
        category = _slug(category_value)
        return f"{prefix}_{category}_{suffix}.{ext}"
    return f"{prefix}_{suffix}.{ext}"


def qc_section_csv_content(section, rows):
    buffer = StringIO()
    writer = csv.writer(buffer)
    headers = QC_SECTION_HEADERS.get(section) or (list(rows[0].keys()) if rows else [])
    writer.writerow(headers)
    for row in rows:
        writer.writerow([row.get(header, "") for header in headers])
    return buffer.getvalue()


def qc_email_attachments(result):
    selected_date = result.get("selected_date") or "selected-date"
    selected_category = result.get("status_category") or "all_categories"
    attachments = []
    for section in QC_EMAIL_ATTACHMENT_SECTIONS:
        tables = export_tables(result, section=section)
        rows = next(iter(tables.values()), [])
        filename = qc_report_filename(section, selected_date, "csv", selected_category)
        attachments.append((filename, qc_section_csv_content(section, rows), "text/csv"))
    return attachments


def _html_card(label, value, color="#0fafbf"):
    return f"""
      <td class="metric-card-cell" style="width:25%;padding:8px;vertical-align:top;">
        <div class="metric-card" style="border:1px solid #e2e8f0;border-radius:10px;padding:14px;background:#ffffff;">
          <div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#64748b;margin-bottom:6px;">{escape(label)}</div>
          <div class="metric-value" style="font-size:22px;font-weight:800;color:{color};line-height:1.2;">{escape(str(value))}</div>
        </div>
      </td>
    """


def _html_kpi_grid(cards):
    first_row = "".join(_html_card(*card) for card in cards[:4])
    second_row = "".join(_html_card(*card) for card in cards[4:8])
    return f"""
      <table role="presentation" cellspacing="0" cellpadding="0" style="width:100%;margin-bottom:14px;">
        <tr class="metric-row">{first_row}</tr>
        <tr class="metric-row">{second_row}</tr>
      </table>
    """


def _html_table(headers, rows):
    header_html = "".join(
        f'<th style="text-align:left;padding:10px;border-bottom:1px solid #e2e8f0;font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.06em;">{escape(str(header))}</th>'
        for header in headers
    )
    row_html = ""
    for row in rows:
        row_html += "<tr>"
        for value in row:
            cell_value = "" if value is None else value
            row_html += f'<td style="padding:10px;border-bottom:1px solid #f1f5f9;font-size:13px;color:#0f172a;">{escape(str(cell_value))}</td>'
        row_html += "</tr>"
    if not rows:
        row_html = f'<tr><td colspan="{len(headers)}" style="padding:18px;color:#64748b;text-align:center;">No data available.</td></tr>'
    return f"""
      <div class="table-scroll" style="width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;">
        <table role="presentation" cellspacing="0" cellpadding="0" style="width:100%;min-width:560px;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:10px;overflow:hidden;background:#ffffff;">
          <thead><tr>{header_html}</tr></thead>
          <tbody>{row_html}</tbody>
        </table>
      </div>
    """


def _image_cell(image_url, fallback_text):
    image_url = str(image_url or "").strip()
    if image_url:
        return f"""
          <td style="width:58px;padding:10px;border-bottom:1px solid #f1f5f9;vertical-align:top;">
            <img src="{escape(image_url)}" alt="{escape(fallback_text)}" width="46" height="46" style="display:block;width:46px;height:46px;object-fit:cover;border-radius:8px;border:1px solid #e2e8f0;">
          </td>
        """
    return """
      <td style="width:58px;padding:10px;border-bottom:1px solid #f1f5f9;vertical-align:top;">
        <div style="width:46px;height:46px;border-radius:8px;border:1px solid #e2e8f0;background:#f8fafc;color:#94a3b8;text-align:center;line-height:46px;font-size:11px;font-weight:800;">SKU</div>
      </td>
    """


def _html_sku_cards(title, rows, metric_label, metric_getter):
    body = ""
    for row in rows[:5]:
        sku = str(row.get("sku") or "-")
        asin = str(row.get("asin") or "-")
        metric_value = metric_getter(row)
        body += f"""
          <tr>
            {_image_cell(row.get("image"), sku)}
            <td style="padding:10px;border-bottom:1px solid #f1f5f9;vertical-align:top;">
              <div style="font-size:13px;font-weight:800;color:#0f172a;line-height:1.35;">{escape(sku)}</div>
              <div style="font-size:11px;color:#64748b;margin-top:3px;">ASIN: {escape(asin)}</div>
            </td>
            <td style="width:120px;padding:10px;border-bottom:1px solid #f1f5f9;text-align:right;vertical-align:top;">
              <div style="font-size:13px;font-weight:800;color:#0fafbf;">{escape(str(metric_value))}</div>
              <div style="font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-top:3px;">{escape(metric_label)}</div>
            </td>
          </tr>
        """
    if not body:
        body = '<tr><td colspan="3" style="padding:18px;color:#64748b;text-align:center;">No data available.</td></tr>'
    return f"""
      <h3 style="font-size:16px;margin:18px 0 10px;color:#0f172a;">{escape(title)}</h3>
      <div class="table-scroll" style="width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;">
        <table role="presentation" cellspacing="0" cellpadding="0" style="width:100%;min-width:520px;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:10px;overflow:hidden;background:#ffffff;">
          <tbody>{body}</tbody>
        </table>
      </div>
    """


def _qc_kpi_cards(result):
    summary = result.get("summary", {})
    return _html_kpi_grid(
        [
            ("Pending Orders", summary.get("pending", 0), "#f59e0b"),
            ("Shipped Orders", summary.get("shipped", 0), "#10b981"),
            ("Shipping Orders", summary.get("shipping", 0), "#0fafbf"),
            ("Cancelled Orders", summary.get("cancelled", 0), "#ef4444"),
            ("Total Revenue", _format_full_money(summary.get("total_revenue", 0)), "#10b981"),
            ("Unique Orders", summary.get("total_orders", 0), "#6366f1"),
            ("Active SKUs", summary.get("active_skus", 0), "#0fafbf"),
            ("QC Alerts", summary.get("qc_alerts", 0), "#ef4444"),
        ]
    )


def _qc_email_sections_html(result):
    return f"""
      {_html_sku_cards(
          "Top 5 SKUs by Order Value",
          result.get("top_value", [])[:5],
          "Order Value",
          lambda row: _format_full_money(row.get("value", 0)),
      )}
      {_html_sku_cards(
          "Top 5 SKUs by Quantity",
          result.get("top_qty", [])[:5],
          "Units",
          lambda row: row.get("qty", 0),
      )}
      <h3 style="font-size:16px;margin:18px 0 10px;color:#0f172a;">Top 5 High Value Orders</h3>
      {_html_table(
          ["Order ID", "ASIN", "Qty", "Order Value", "Flag"],
          [
              [
                  row.get("order_id", ""),
                  row.get("asin", ""),
                  row.get("qty", 0),
                  _format_full_money(row.get("value", 0)),
                  row.get("flag", ""),
              ]
              for row in result.get("bulk_orders", [])[:5]
          ],
      )}
      <h3 style="font-size:16px;margin:18px 0 10px;color:#0f172a;">Top 5 Orders Needing QC Check</h3>
      {_html_table(
          ["ASIN", "SKU", "Qty", "Order Value"],
          [
              [
                  row.get("asin", ""),
                  row.get("sku", ""),
                  row.get("qty", 0),
                  _format_full_money(row.get("value", 0)),
              ]
              for row in result.get("qc_alerts", [])[:5]
          ],
      )}
      <h3 style="font-size:16px;margin:18px 0 10px;color:#0f172a;">Top 5 Orders by Category</h3>
      {_html_table(
          ["Category", "Orders", "Units", "Revenue", "Share"],
          [
              [
                  row.get("category", ""),
                  row.get("orders", 0),
                  row.get("qty", 0),
                  _format_full_money(row.get("value", 0)),
                  f"{row.get('share', 0)}%",
              ]
              for row in result.get("category_rows", [])[:5]
          ],
      )}
      <h3 style="font-size:16px;margin:18px 0 10px;color:#0f172a;">Top 5 Orders by Hour (IST)</h3>
      {_html_table(
          ["Hour", "Orders", "Order Value", "Share"],
          [
              [
                  row.get("label", ""),
                  row.get("orders", 0),
                  _format_full_money(row.get("value", 0)),
                  f"{row.get('share', 0)}%",
              ]
              for row in result.get("hourly_rows", [])[:5]
          ],
      )}
    """


def _email_shell(title, subtitle, cards_html, body_html):
    return f"""
    <!doctype html>
    <html>
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
      <style>
        body {{
          margin: 0 !important;
          padding: 0 !important;
          background: #f8fafc !important;
        }}
        table {{
          border-spacing: 0;
        }}
        img {{
          max-width: 100%;
        }}
        @media only screen and (max-width: 640px) {{
          .email-page {{
            padding: 12px !important;
          }}
          .email-container {{
            width: 100% !important;
            max-width: 100% !important;
            border-radius: 10px !important;
          }}
          .email-header {{
            padding: 18px !important;
          }}
          .email-title {{
            font-size: 19px !important;
          }}
          .email-body {{
            padding: 12px !important;
          }}
          .metric-row,
          .metric-card-cell {{
            display: block !important;
            width: 100% !important;
          }}
          .metric-card-cell {{
            padding: 6px 0 !important;
          }}
          .metric-card {{
            padding: 12px !important;
          }}
          .metric-value {{
            font-size: 20px !important;
          }}
          .table-scroll {{
            overflow-x: auto !important;
          }}
          .email-footer {{
            padding-left: 12px !important;
            padding-right: 12px !important;
          }}
        }}
        @media only screen and (min-width: 641px) and (max-width: 900px) {{
          .email-container {{
            max-width: 92% !important;
          }}
          .email-page {{
            padding: 18px !important;
          }}
        }}
      </style>
    </head>
    <body>
    <div class="email-page" style="margin:0;padding:24px;background:#f8fafc;font-family:Arial,Helvetica,sans-serif;color:#0f172a;">
      <div class="email-container" style="width:100%;max-width:760px;margin:0 auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:14px;overflow:hidden;">
        <div class="email-header" style="padding:22px 26px;background:#0fafbf;color:#ffffff;">
          <div class="email-title" style="font-size:22px;font-weight:800;line-height:1.2;">{escape(title)}</div>
          <div style="font-size:13px;opacity:.9;margin-top:6px;line-height:1.45;">{escape(subtitle)}</div>
        </div>
        <div class="email-body" style="padding:18px;">
          {cards_html}
          {body_html}
          <div style="margin-top:18px;padding:14px;border-radius:10px;background:#eff6ff;color:#334155;font-size:13px;">
            All QC Automation reports are attached as CSV files: high value orders, QC check orders, category breakdown, hourly orders, and daily status.
          </div>
        </div>
      </div>
      <div class="email-footer" style="max-width:760px;margin:12px auto 0;text-align:center;color:#94a3b8;font-size:12px;">Sent by System</div>
    </div>
    </body>
    </html>
    """


def parse_qc_uploads(order_file, image_file, category_file=None):
    orders = _read_combined_tables(order_file, "30-Day Order Report")
    _require_columns(
        orders,
        [
            "amazon-order-id",
            "purchase-date",
            "order-status",
            "product-name",
            "sku",
            "asin",
            "quantity",
            "item-price",
        ],
        "30-Day Order Report",
    )

    image_map = {}
    for image_upload in _as_file_list(image_file):
        images = _normalize_columns(_read_table(image_upload))
        asin_col = "asin"
        image_col = "image link" if "image link" in images.columns else "image url"
        _require_columns(images, [asin_col, image_col], "ASIN + Image Report")
        for row in images.to_dict("records"):
            asin = str(row.get(asin_col, "")).strip()
            image = str(row.get(image_col, "")).strip()
            if asin and image:
                image_map[asin] = image

    category_map = {}
    for category_upload in _as_file_list(category_file):
        categories = _normalize_columns(_read_table(category_upload))
        _require_columns(categories, ["asin", "portfolio", "category"], "Category Mapping")
        for row in categories.to_dict("records"):
            asin = str(row.get("asin", "")).strip()
            if asin:
                category_map[asin] = {
                    "portfolio": str(row.get("portfolio", "")).strip(),
                    "category": str(row.get("category", "")).strip() or "Unmapped",
                    "subcategory": str(row.get("subcategory", "")).strip(),
                    "sku": str(row.get("skus", "")).strip(),
                }

    rows = []
    for row in orders.to_dict("records"):
        asin = str(row.get("asin", "")).strip()
        sku = str(row.get("sku", "")).strip()
        if not asin or not sku:
            continue
        ist_dt = _ist_datetime(row.get("purchase-date"))
        if ist_dt is None:
            continue
        cat = category_map.get(asin, {})
        rows.append(
            {
                "order_id": str(row.get("amazon-order-id", "")).strip(),
                "purchase_date": str(row.get("purchase-date", "")).strip(),
                "ist_date": ist_dt.strftime("%Y-%m-%d"),
                "ist_display": _display_ist(ist_dt),
                "hour": int(ist_dt.hour),
                "status": str(row.get("order-status", "")).strip(),
                "product_name": str(row.get("product-name", "")).strip(),
                "sku": sku,
                "asin": asin,
                "qty": _to_int(row.get("quantity")),
                "value": _to_float(row.get("item-price")),
                "image": image_map.get(asin, ""),
                "portfolio": cat.get("portfolio", ""),
                "category": cat.get("category", "Unmapped"),
                "subcategory": cat.get("subcategory", ""),
            }
        )

    if not rows:
        raise ValueError("No valid order rows found in the uploaded report.")

    dates = sorted({row["ist_date"] for row in rows})
    return {
        "rows": rows,
        "date_options": dates,
        "start_date": dates[0],
        "end_date": dates[-1],
        "row_count": len(rows),
        "image_count": len(image_map),
        "category_count": len(category_map),
        "status_overview": build_status_overview(rows),
    }


def _safe_pct(part, whole):
    return round((float(part or 0) / float(whole or 1)) * 100, 1) if whole else 0


def analyze_qc_dataset(
    dataset,
    selected_date=None,
    projected_revenue=DEFAULT_REVENUE_CAP,
    revenue_cap=DEFAULT_REVENUE_CAP,
    status_category=None,
):
    rows = list(dataset.get("rows") or [])
    date_options = list(dataset.get("date_options") or [])
    if not date_options:
        raise ValueError("No dates are available in this QC dataset.")

    selected_date = str(selected_date or "").strip()
    try:
        datetime.date.fromisoformat(selected_date)
    except ValueError:
        selected_date = date_options[-1]
    selected_day = datetime.date.fromisoformat(selected_date)
    prior_start = (selected_day - datetime.timedelta(days=30)).isoformat()

    day_rows = [row for row in rows if row["ist_date"] == selected_date]
    prior_rows = [
        row
        for row in rows
        if row.get("ist_date") and prior_start <= row["ist_date"] < selected_date
    ]
    prior_asins = {row["asin"] for row in prior_rows}
    active_rows = [row for row in day_rows if not _is_cancelled(row.get("status"))]

    by_sku = {}
    for row in active_rows:
        key = f"{row['sku']}||{row['asin']}"
        item = by_sku.setdefault(
            key,
            {
                "sku": row["sku"],
                "asin": row["asin"],
                "product_name": row["product_name"],
                "image": row.get("image", ""),
                "value": 0.0,
                "qty": 0,
                "orders": set(),
            },
        )
        item["value"] += row["value"]
        item["qty"] += row["qty"]
        item["orders"].add(row["order_id"])

    sku_rows = []
    for item in by_sku.values():
        cleaned = dict(item)
        cleaned["orders"] = len(item["orders"])
        cleaned["value_display"] = _format_money(item["value"])
        sku_rows.append(cleaned)

    top_value = sorted(sku_rows, key=lambda item: item["value"], reverse=True)[:10]
    top_qty = sorted(sku_rows, key=lambda item: item["qty"], reverse=True)[:10]

    bulk_map = {}
    for row in active_rows:
        if row["qty"] >= BULK_QTY_THRESHOLD or row["value"] >= HIGH_VALUE_THRESHOLD:
            key = f"{row['order_id']}||{row['sku']}"
            item = bulk_map.setdefault(
                key,
                {
                    "order_id": row["order_id"],
                    "order_date_ist": row["ist_display"],
                    "sku": row["sku"],
                    "asin": row["asin"],
                    "value": 0.0,
                    "qty": 0,
                },
            )
            item["value"] += row["value"]
            item["qty"] += row["qty"]
    bulk_orders = sorted(bulk_map.values(), key=lambda item: item["value"], reverse=True)
    for item in bulk_orders:
        high_value = item["value"] >= HIGH_VALUE_THRESHOLD
        high_qty = item["qty"] >= BULK_QTY_THRESHOLD
        item["flag"] = (
            "High Value + Bulk Qty"
            if high_value and high_qty
            else "High Value"
            if high_value
            else "Bulk Qty"
        )

    qc_alerts = [
        item
        for item in sku_rows
        if item["asin"] and item["asin"] not in prior_asins
    ]
    qc_message = (
        f"{len(qc_alerts)} ASINs ordered on this date had no orders in the previous "
        "30 days in this file - flag for QC before dispatch."
    )
    qc_alerts.sort(key=lambda item: item["value"], reverse=True)

    status_sets = {
        "pending": set(),
        "shipped": set(),
        "shipping": set(),
        "cancelled": set(),
    }
    for row in day_rows:
        status = str(row.get("status", "")).strip().lower()
        if status == "pending":
            status_sets["pending"].add(row["order_id"])
        elif status == "shipped":
            status_sets["shipped"].add(row["order_id"])
        elif status == "shipping":
            status_sets["shipping"].add(row["order_id"])
        elif _is_cancelled(status):
            status_sets["cancelled"].add(row["order_id"])

    category_map = {}
    for row in active_rows:
        category = row.get("category") or "Unmapped"
        item = category_map.setdefault(
            category,
            {
                "portfolio": row.get("portfolio", ""),
                "category": category,
                "orders": set(),
                "asins": {},
                "qty": 0,
                "value": 0.0,
            },
        )
        item["orders"].add(row["order_id"])
        item["qty"] += row["qty"]
        item["value"] += row["value"]
        asin_item = item["asins"].setdefault(
            row["asin"],
            {
                "portfolio": row.get("portfolio", ""),
                "category": category,
                "subcategory": row.get("subcategory", ""),
                "asin": row["asin"],
                "orders": set(),
                "qty": 0,
                "value": 0.0,
            },
        )
        asin_item["orders"].add(row["order_id"])
        asin_item["qty"] += row["qty"]
        asin_item["value"] += row["value"]

    category_rows = []
    max_category_orders = 1
    for item in category_map.values():
        max_category_orders = max(max_category_orders, len(item["orders"]))
    for item in category_map.values():
        asins = []
        for asin_item in item["asins"].values():
            cleaned_asin = dict(asin_item)
            cleaned_asin["orders"] = len(asin_item["orders"])
            asins.append(cleaned_asin)
        category_rows.append(
            {
                "portfolio": item["portfolio"],
                "category": item["category"],
                "asin_count": len(item["asins"]),
                "orders": len(item["orders"]),
                "qty": item["qty"],
                "value": item["value"],
                "share": _safe_pct(len(item["orders"]), max_category_orders),
                "asins": sorted(asins, key=lambda asin: asin["value"], reverse=True),
            }
        )
    category_rows.sort(key=lambda item: item["orders"], reverse=True)

    hourly_map = {}
    for row in active_rows:
        hour = row["hour"]
        label = f"{hour:02d}:00 - {hour:02d}:59"
        item = hourly_map.setdefault(
            hour,
            {"hour": hour, "label": label, "orders": set(), "value": 0.0, "lines": []},
        )
        item["orders"].add(row["order_id"])
        item["value"] += row["value"]
        item["lines"].append(
            {
                "order_id": row["order_id"],
                "asin": row["asin"],
                "qty": row["qty"],
                "value": row["value"],
            }
        )
    max_hour_orders = max((len(item["orders"]) for item in hourly_map.values()), default=1)
    hourly_rows = []
    for item in sorted(hourly_map.values(), key=lambda value: value["hour"]):
        hourly_rows.append(
            {
                "hour": item["hour"],
                "label": item["label"],
                "orders": len(item["orders"]),
                "value": item["value"],
                "share": _safe_pct(len(item["orders"]), max_hour_orders),
                "lines": item["lines"],
            }
        )

    total_revenue = sum(item["value"] for item in sku_rows)
    projected_revenue = _to_float(projected_revenue) or DEFAULT_REVENUE_CAP
    revenue_cap = _to_float(revenue_cap) or DEFAULT_REVENUE_CAP
    capped_revenue = min(projected_revenue, revenue_cap)
    threshold_value = capped_revenue * 0.5

    category_options = sorted(
        {
            str(row.get("category") or "").strip()
            for row in rows
            if str(row.get("category") or "").strip()
        }
    )
    status_category = (
        status_category
        if status_category and status_category in category_options
        else "__all__"
    )

    result = {
        "selected_date": selected_date,
        "status_category": status_category,
        "category_options": category_options,
        "date_options": date_options,
        "summary": {
            "row_count": dataset.get("row_count", len(rows)),
            "day_count": len(date_options),
            "date_range": f"{dataset.get('start_date')} to {dataset.get('end_date')}",
            "total_revenue": total_revenue,
            "total_revenue_display": _format_money(total_revenue),
            "total_orders": len({row["order_id"] for row in active_rows}),
            "active_skus": len(sku_rows),
            "qc_alerts": len(qc_alerts),
            "pending": len(status_sets["pending"]),
            "shipped": len(status_sets["shipped"]),
            "shipping": len(status_sets["shipping"]),
            "cancelled": len(status_sets["cancelled"]),
        },
        "threshold": {
            "projected_revenue": projected_revenue,
            "revenue_cap": revenue_cap,
            "capped_revenue": capped_revenue,
            "threshold_value": threshold_value,
            "revenue_pct": _safe_pct(total_revenue, capped_revenue),
            "crossed": total_revenue >= threshold_value,
        },
        "top_value": top_value,
        "top_qty": top_qty,
        "bulk_orders": bulk_orders,
        "qc_alerts": qc_alerts,
        "qc_message": qc_message,
        "category_rows": category_rows,
        "hourly_rows": hourly_rows,
        "status_overview": build_status_overview(
            rows,
            None if status_category == "__all__" else status_category,
        ),
    }
    return result


def build_status_overview(rows, category_filter=None):
    date_map = {}
    for row in rows:
        if category_filter and row.get("category") != category_filter:
            continue
        date = row.get("ist_date")
        if not date:
            continue
        item = date_map.setdefault(
            date, {"date": date, "shipped": set(), "pending": set(), "cancelled": set()}
        )
        status = str(row.get("status", "")).strip().lower()
        if status == "shipped":
            item["shipped"].add(row["order_id"])
        elif status == "pending":
            item["pending"].add(row["order_id"])
        elif _is_cancelled(status):
            item["cancelled"].add(row["order_id"])

    overview = []
    for item in sorted(date_map.values(), key=lambda value: value["date"]):
        shipped = len(item["shipped"])
        pending = len(item["pending"])
        cancelled = len(item["cancelled"])
        overview.append(
            {
                "date": item["date"],
                "shipped": shipped,
                "pending": pending,
                "cancelled": cancelled,
                "total": shipped + pending + cancelled,
            }
        )
    return overview


def export_tables(result, section=None):
    bulk_rows = [
        {
            "Order ID": row["order_id"],
            "Order Date (IST)": row["order_date_ist"],
            "Order Value (₹)": row["value"],
            "Order Qty": row["qty"],
            "SKU": row["sku"],
            "ASIN": row["asin"],
            "Flag": row["flag"],
        }
        for row in result.get("bulk_orders", [])
        if row.get("value", 0) >= HIGH_VALUE_THRESHOLD
        or row.get("qty", 0) >= BULK_QTY_THRESHOLD
    ]
    qc_rows = [
        {
            "ASIN": row["asin"],
            "SKU": row["sku"],
            "Product Name": row.get("product_name", ""),
            "Qty Ordered": row["qty"],
            "Order Value (₹)": row["value"],
        }
        for row in result.get("qc_alerts", [])
    ]
    category_rows = []
    for category in result.get("category_rows", []):
        for asin in category.get("asins", []):
            category_rows.append(
                {
                    "Portfolio": asin.get("portfolio") or category.get("portfolio"),
                    "Category": category.get("category"),
                    "Subcategory": asin.get("subcategory", ""),
                    "ASIN": asin.get("asin"),
                    "Orders": asin.get("orders"),
                    "Units": asin.get("qty"),
                    "Revenue": asin.get("value"),
                }
            )
    hourly_rows = []
    for hour in result.get("hourly_rows", []):
        for line in hour.get("lines", []):
            hourly_rows.append(
                {
                    "Hour (IST)": hour.get("label"),
                    "Order ID": line.get("order_id"),
                    "ASIN": line.get("asin"),
                    "Quantity": line.get("qty"),
                    "Order Value (₹)": line.get("value"),
                }
            )
    status_rows = [
        {
            "Date": row.get("date"),
            "Shipped": row.get("shipped"),
            "Pending": row.get("pending"),
            "Cancelled": row.get("cancelled"),
            "Total Orders": row.get("total"),
        }
        for row in result.get("status_overview", [])
    ]
    tables = {
        "bulk": ("High Value Orders", bulk_rows),
        "qc": ("Orders Needing QC Check", qc_rows),
        "category": ("Orders by Category", category_rows),
        "hourly": ("Orders by Hour", hourly_rows),
        "status": ("Daily Order Status", status_rows),
    }
    section = str(section or "").strip().lower()
    if section:
        if section not in tables:
            return {}
        name, rows = tables[section]
        return {name: rows}
    return {name: rows for name, rows in tables.values()}


def qc_email_body(result):
    lines = [
        f"QC Automation alerts for {result.get('selected_date')}",
        "",
        f"QC alert ASINs: {len(result.get('qc_alerts', []))}",
        f"Bulk / high-value order lines: {len(result.get('bulk_orders', []))}",
        f"Total revenue: Rs. {result['summary']['total_revenue']:,.0f}",
        "",
        "Top QC alerts:",
    ]
    for row in result.get("qc_alerts", [])[:10]:
        lines.append(
            f"- {row['asin']} | {row['sku']} | qty {row['qty']} | Rs. {row['value']:,.0f}"
        )
    if not result.get("qc_alerts"):
        lines.append("- No QC alerts for the selected date.")
    return "\n".join(lines)


def qc_email_html(result):
    body = f"""
      <div style="font-size:14px;line-height:1.6;color:#334155;margin-bottom:16px;">
        Please review the orders below for QC priority. These include first-order ASIN checks, high quantity orders, and high value orders for the selected date.
      </div>
      {_qc_email_sections_html(result)}
    """
    return _email_shell(
        f"QC Action Required - {result.get('selected_date')}",
        "Order quality checks for first-order ASINs, bulk quantity, and high-value orders",
        _qc_kpi_cards(result),
        body,
    )


def category_email_body(result):
    threshold = result.get("threshold", {})
    lines = [
        f"Category revenue alert for {result.get('selected_date')}",
        "",
        f"Revenue: Rs. {result['summary']['total_revenue']:,.0f}",
        f"50% threshold: Rs. {threshold.get('threshold_value', 0):,.0f}",
        f"Projected revenue used: Rs. {threshold.get('capped_revenue', 0):,.0f}",
        f"Progress: {threshold.get('revenue_pct', 0)}%",
        "",
        "Top categories:",
    ]
    for row in result.get("category_rows", [])[:10]:
        lines.append(
            f"- {row['category']} | orders {row['orders']} | units {row['qty']} | Rs. {row['value']:,.0f}"
        )
    return "\n".join(lines)


def category_email_html(result):
    threshold = result.get("threshold", {})
    body = f"""
      <div style="font-size:14px;line-height:1.6;color:#334155;margin-bottom:16px;">
        Revenue has crossed the configured alert threshold. Current progress is <strong>{threshold.get('revenue_pct', 0)}%</strong> against a threshold of <strong>{_format_full_money(threshold.get("threshold_value", 0))}</strong>.
      </div>
      {_qc_email_sections_html(result)}
    """
    return _email_shell(
        f"Category Revenue Alert - {result.get('selected_date')}",
        "Revenue threshold crossed for the selected QC Automation date",
        _qc_kpi_cards(result),
        body,
    )


def tables_to_excel_bytes(tables):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        wrote = False
        for sheet_name, rows in tables.items():
            safe_name = sheet_name[:31]
            df = pd.DataFrame(rows)
            if df.empty:
                df = pd.DataFrame({"Message": ["No data available"]})
            df.to_excel(writer, sheet_name=safe_name, index=False)
            wrote = True
        if not wrote:
            pd.DataFrame({"Message": ["No data available"]}).to_excel(
                writer, sheet_name="QC Automation", index=False
            )
    output.seek(0)
    return output
