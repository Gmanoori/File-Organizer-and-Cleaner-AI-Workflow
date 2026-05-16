# Gemini Data Consolidation Analysis

## 1. Additional Consolidation Opportunities
Beyond the existing merge rules, the following column groups should be consolidated to reduce schema complexity:

### Geographic Metadata
- **Group:** `state_id`, `city_id`, `country_code`, `country_dial_code`, `area_code`
- **Reason:** These are derived attributes that can be normalized into a master lookup table to save space.

### Web & Digital Presence
- **Group:** `website`, `indiamart_url`, `profile_image_url`, `url`, `e-mail/ Web Page`
- **Reason:** These frequently contain overlapping data. `e-mail/ Web Page` in older schemas acts as a catch-all.

### Business Intelligence / Descriptions
- **Group:** `short_description`, `long_description`, `Remarks`, `Product/Service`
- **Reason:** Descriptions are often repeated. `long_description` often contains boilerplate text wrapped around the `short_description`.

### Indiamart Schema Alignment (Generic Headers)
- **Mapping:** 
    - `data_col_18` + `data_col_19` -> `contact_first_name` / `contact_last_name` (Verified in 70+ column schemas)
    - `data_col_31` -> `landmark`
    - `data_col_68` -> `full_address_backup`

---

## 2. Join Strategy
To merge files from different batches while maintaining integrity, use this hierarchy:

| Priority | Column(s) | Confidence | Use Case |
| :--- | :--- | :--- | :--- |
| **P0** | `company_id` | High | Indiamart internal tracking (unique per business entity). |
| **P1** | `email_1` (Normalized) | Medium | Cross-source identification. |
| **P2** | `slug` | Medium | Persistent URL identifier. |
| **P3** | `company_name` + `pincode` | Low | Fuzzy matching for non-Indiamart sources. |

---

## 3. Risks & Critical Problems

### A. Phone Number Artifacts (The "Negative" Bug)
- **Observation:** Phone numbers like `9712070345` are stored in some columns as `-9712070254`.
- **Proof:** Seen in `FILE_000028_bb14b9d8_new.csv`.
- **Impact:** Joins on phone numbers will fail unless absolute values are taken or the `-` is stripped.

### B. Cartesian Product Blowup
- **Risk:** `company_id` is not always a primary key in every file. If a file contains revision history, joining on `company_id` will cause a massive row expansion.
- **Mitigation:** De-duplicate files by `company_id` + `last_updated` before joining.

### C. Generic Header Drift
- **Risk:** `data_col_13` in a file with 47 columns represents a different field than in a file with 75 columns.
- **Proof:** Comparison of `FILE_002113` (47 cols) and `FILE_000028` (75 cols) shows index misalignment for generic labels.
- **Impact:** Blind merging of generic columns will corrupt data.

### D. Address Divergence
- **Risk:** Files often contain both `address_line_1` and a concatenated `full_address`.
- **Problem:** Updating one without the other leads to "split-brain" data where the components don't match the whole.

---

## 4. Edge Cases
1. **Pincode Padding:** Leading zeros in Delhi/Cuttack (e.g., `011`) might be lost if handled as integers.
2. **Amount Confusion:** Columns like `amount_1`, `amount_2` usually contain internal IDs or sequence numbers (e.g., `42095`), not financial values.
3. **Empty Strings vs Nulls:** Consistency in representing missing data is required before any `GROUP BY` or `JOIN` operation.
