# Admin Workflow Guide: Plantex Sales Dashboard

This document provides a guide on how to manage data through the Django Admin interface (`/admin`). It includes descriptions of the models, the data required, and practical examples for each.

## 1. Accounts & Permissions

Managing users and their access is the first step in setting up the dashboard.

### **Features**
Features represent specific sections or capabilities of the dashboard (e.g., "Upload Data", "CEO Dashboard").

| Field | Description | Example |
| :--- | :--- | :--- |
| **Name** | Display name for the feature. | `Upload Data` |
| **Code Name** | Unique internal ID used in code. | `upload_data` |

**Example Feature List:**
- Name: `CEO Dashboard`, Code Name: `ceo_dashboard`
- Name: `Business Dashboard`, Code Name: `business_dashboard`
- Name: `Replenishment`, Code Name: `replenishment`

---

### **Roles**
Roles group Features together. You assign a Role to a user to define what they can see.

| Field | Description | Example |
| :--- | :--- | :--- |
| **Name** | Name of the role. | `Business Head` |
| **Features** | Select one or more Features from the list. | `Business Dashboard`, `Upload Data` |

---

### **Users**
Individual user accounts.

| Field | Description | Example |
| :--- | :--- | :--- |
| **First Name (fname)** | User's first name. | `John` |
| **Last Name (lname)** | User's last name. | `Doe` |
| **Email** | Unique login email. | `john.doe@plantex.in` |
| **Password (pswd)** | Plain text password. | `password123` |
| **Is Main User** | Check this for the primary account owner. | `True` |
| **Role** | The assigned Role (e.g., Business Head). | `Business Head` |

---

## 2. Dashboard Data Management

Most data is uploaded via the dashboard UI, but you can manually edit mappings or prices here.

### **Category Mappings**
Maps an ASIN to a specific Category and Portfolio. This is used for the "Category Dashboard".

| Field | Description | Example |
| :--- | :--- | :--- |
| **User** | The owner of this mapping. | `Select User...` |
| **ASIN** | Amazon Standard Identification Number. | `B07XJ8L6M1` |
| **Portfolio** | High-level grouping. | `Home Decor` |
| **Category** | Specific category. | `Wall Shelves` |
| **Subcategory** | Further classification. | `Wooden Shelves` |

---

### **Price Data**
Stores the current price for an ASIN.

| Field | Description | Example |
| :--- | :--- | :--- |
| **User** | The owner of this data. | `Select User...` |
| **ASIN** | Amazon ASIN. | `B07XJ8L6M1` |
| **Price** | Current selling price. | `499.00` |

---

## 3. Flipkart & Ads Reports

These tables store raw data from uploaded reports. Usually, you don't need to add these manually, but you can edit them if there's a typo in the source data.

### **Flipkart Sales Report**
- **Order ID**: `OD1234567890`
- **SKU**: `PL-SHELF-01`
- **Final Invoice Amount**: `599.00`

### **PCA/PLA Reports (Ads)**
- **Campaign Name**: `Summer Sale 2024`
- **Direct Revenue**: `15000.50`
- **Clicks**: `450`

---

## 4. Admin Best Practices

1. **Hierarchy First**: Create **Features** first, then **Roles**, and finally **Users**.
2. **Naming Convention**: Use lowercase and underscores for `code_name` in Features (e.g., `upload_data`).
3. **User Relationships**: Always ensure that added data (like Mappings) is linked to the correct **User** so it shows up on their specific dashboard.
