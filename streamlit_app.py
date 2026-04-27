import streamlit as st
import pandas as pd
import json
import tempfile
import os
from datetime import date

st.set_page_config(
    page_title="POD Document Processing",
    page_icon="🚚",
    layout="wide",
)

conn = st.connection("snowflake")
session = conn.session()

def ensure_review_table():
    session.sql("""
        CREATE TABLE IF NOT EXISTS AI_PLATFORM_DEV.CURATED.POD_REVIEW_LOG (
            REVIEW_ID VARCHAR(50),
            POD_ID VARCHAR(20),
            FILE_NAME VARCHAR(500),
            REVIEW_STATUS VARCHAR(20),
            REVIEWER VARCHAR(200),
            REVIEW_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            AI_CONFIDENCE FLOAT,
            FIELDS_MODIFIED VARCHAR(2000),
            REVIEW_NOTES VARCHAR(1000),
            RAW_AI_OUTPUT VARIANT,
            FINAL_OUTPUT VARIANT
        )
    """).collect()

ensure_review_table()

st.title("🚚 POD document processing")
st.caption("Use Case 1 — Upload a POD PDF, extract structured data with Cortex AI, review, and approve")

tab_upload, tab_review, tab_dashboard = st.tabs([
    "📄 Process & review POD",
    "📋 Review queue",
    "📊 POD analytics dashboard",
])

STATUS_OPTIONS = [
    "Delivered - Clean",
    "Delivered - With Exceptions",
    "Partial Delivery",
    "Refused",
    "Undeliverable",
]

with tab_upload:
    col_upload, col_results = st.columns([1, 2])

    with col_upload:
        st.subheader("Upload POD")
        uploaded_file = st.file_uploader(
            "Drop a POD PDF here",
            type=["pdf", "png", "jpg", "jpeg"],
            help="Supported: PDF, PNG, JPEG",
        )
        parse_mode = st.radio(
            "Extraction mode",
            ["LAYOUT", "OCR"],
            index=0,
            horizontal=True,
            help="LAYOUT preserves tables/structure. OCR is faster for scanned docs.",
        )
        process_btn = st.button(
            "▶ Process document",
            type="primary",
            use_container_width=True,
            disabled=uploaded_file is None,
        )

    with col_results:
        if process_btn and uploaded_file is not None:
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=os.path.splitext(uploaded_file.name)[1]
            ) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name

            with st.status("Processing POD document...", expanded=True) as status:
                st.write("⬆️ Uploading to Snowflake stage...")
                session.sql(
                    "CREATE STAGE IF NOT EXISTS AI_PLATFORM_DEV.RAW.POD_DEMO_STAGE "
                    "DIRECTORY = (ENABLE = TRUE)"
                ).collect()
                session.file.put(
                    tmp_path,
                    "@AI_PLATFORM_DEV.RAW.POD_DEMO_STAGE",
                    auto_compress=False,
                    overwrite=True,
                )
                staged_filename = os.path.basename(tmp_path)
                os.unlink(tmp_path)

                st.write(f"🔍 Running AI_PARSE_DOCUMENT ({parse_mode} mode)...")
                parse_result = session.sql(f"""
                    SELECT AI_PARSE_DOCUMENT(
                        TO_FILE('@AI_PLATFORM_DEV.RAW.POD_DEMO_STAGE', '{staged_filename}'),
                        {{'mode': '{parse_mode}'}}
                    ) AS parsed
                """).collect()

                parsed = json.loads(parse_result[0]["PARSED"])

                if parsed.get("errorInformation"):
                    status.update(label="Error", state="error")
                    st.error(f"Parsing failed: {parsed['errorInformation']}")
                    st.stop()

                raw_text = parsed.get("content", "")
                page_count = parsed.get("metadata", {}).get("pageCount", 0)
                st.write(f"✅ Extracted {page_count} page(s), {len(raw_text):,} characters")

                st.write("🧠 Running AI_EXTRACT for structured fields...")
                safe_text = raw_text.replace("'", "''").replace("\\", "\\\\")
                extract_result = session.sql(f"""
                    SELECT AI_EXTRACT(
                        '{safe_text}',
                        OBJECT_CONSTRUCT(
                            'pod_reference', 'POD reference ID',
                            'shipment_id', 'Shipment or tracking ID',
                            'order_id', 'Order ID',
                            'customer_name', 'Customer or company name',
                            'carrier_name', 'Carrier or courier company name',
                            'delivery_date', 'Date of delivery (YYYY-MM-DD)',
                            'signed_by', 'Person who signed for delivery',
                            'receiver_name', 'Name or title of receiver',
                            'delivery_address', 'Full delivery address',
                            'delivery_city', 'City of delivery',
                            'delivery_state', 'State of delivery',
                            'delivery_status', 'Delivery status',
                            'exception_notes', 'Any exception or damage notes',
                            'signature_present', 'Whether signature is present (Yes/No)',
                            'damage_description', 'Description of any damage',
                            'partial_delivery', 'Whether partial delivery (Yes/No)',
                            'packages_received', 'Number of packages received',
                            'packages_expected', 'Number of packages expected'
                        )
                    ) AS fields
                """).collect()

                fields = json.loads(extract_result[0]["FIELDS"])
                status.update(label="Processing complete — review below", state="complete")

            st.session_state["extracted_fields"] = fields
            st.session_state["raw_text"] = raw_text
            st.session_state["source_file"] = uploaded_file.name

        if "extracted_fields" in st.session_state:
            fields = st.session_state["extracted_fields"]
            raw_text = st.session_state["raw_text"]

            st.divider()
            st.subheader("👤 Human review")
            st.caption("Verify and correct the AI-extracted fields before approving. Modified fields are tracked in the audit log.")

            with st.form("review_form"):
                st.markdown("**Shipment details**")
                r1c1, r1c2, r1c3, r1c4 = st.columns(4)
                pod_ref = r1c1.text_input("POD reference", value=fields.get("pod_reference", ""))
                shipment_id = r1c2.text_input("Shipment ID", value=fields.get("shipment_id", ""))
                order_id = r1c3.text_input("Order ID", value=fields.get("order_id", ""))
                delivery_date = r1c4.text_input("Delivery date", value=fields.get("delivery_date", ""))

                st.markdown("**Parties**")
                r2c1, r2c2, r2c3, r2c4 = st.columns(4)
                customer_name = r2c1.text_input("Customer", value=fields.get("customer_name", ""))
                carrier_name = r2c2.text_input("Carrier", value=fields.get("carrier_name", ""))
                signed_by = r2c3.text_input("Signed by", value=fields.get("signed_by", ""))
                receiver_name = r2c4.text_input("Receiver", value=fields.get("receiver_name", ""))

                st.markdown("**Location**")
                r3c1, r3c2, r3c3 = st.columns(3)
                delivery_address = r3c1.text_input("Address", value=fields.get("delivery_address", ""))
                delivery_city = r3c2.text_input("City", value=fields.get("delivery_city", ""))
                delivery_state = r3c3.text_input("State", value=fields.get("delivery_state", ""))

                st.markdown("**Delivery status & exceptions**")
                r4c1, r4c2, r4c3 = st.columns(3)
                ai_status = fields.get("delivery_status", "")
                status_idx = STATUS_OPTIONS.index(ai_status) if ai_status in STATUS_OPTIONS else 0
                delivery_status = r4c1.selectbox("Delivery status", STATUS_OPTIONS, index=status_idx)

                ai_sig = str(fields.get("signature_present", "")).lower() in ("yes", "true", "1")
                signature_present = r4c2.selectbox("Signature present", ["Yes", "No"], index=0 if ai_sig else 1)

                ai_partial = str(fields.get("partial_delivery", "")).lower() in ("yes", "true", "1")
                partial_delivery = r4c3.selectbox("Partial delivery", ["No", "Yes"], index=1 if ai_partial else 0)

                r5c1, r5c2 = st.columns(2)
                pkgs_received = r5c1.text_input("Packages received", value=str(fields.get("packages_received", "")))
                pkgs_expected = r5c2.text_input("Packages expected", value=str(fields.get("packages_expected", "")))

                exception_notes = st.text_area("Exception notes", value=fields.get("exception_notes", "") or "")
                damage_description = st.text_area("Damage description", value=fields.get("damage_description", "") or "")

                st.divider()
                st.markdown("**Review decision**")
                rev_c1, rev_c2 = st.columns([2, 1])
                reviewer_name = rev_c1.text_input("Reviewer name", value="")
                review_notes = st.text_area("Review notes (optional)", value="", placeholder="Any comments for the audit trail...")

                btn_c1, btn_c2, btn_c3 = st.columns(3)
                approve = btn_c1.form_submit_button("✅ Approve & save", type="primary", use_container_width=True)
                reject = btn_c2.form_submit_button("❌ Reject", use_container_width=True)
                flag = btn_c3.form_submit_button("🔶 Flag for re-review", use_container_width=True)

            if approve or reject or flag:
                if not reviewer_name.strip():
                    st.error("Reviewer name is required.")
                    st.stop()

                review_status = "APPROVED" if approve else ("REJECTED" if reject else "FLAGGED")

                final_fields = {
                    "pod_reference": pod_ref,
                    "shipment_id": shipment_id,
                    "order_id": order_id,
                    "customer_name": customer_name,
                    "carrier_name": carrier_name,
                    "delivery_date": delivery_date,
                    "signed_by": signed_by,
                    "receiver_name": receiver_name,
                    "delivery_address": delivery_address,
                    "delivery_city": delivery_city,
                    "delivery_state": delivery_state,
                    "delivery_status": delivery_status,
                    "signature_present": signature_present,
                    "partial_delivery": partial_delivery,
                    "packages_received": pkgs_received,
                    "packages_expected": pkgs_expected,
                    "exception_notes": exception_notes,
                    "damage_description": damage_description,
                }

                modified = [
                    k for k in final_fields
                    if str(final_fields[k]).strip() != str(fields.get(k, "")).strip()
                ]

                review_id = f"REV-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
                safe_ai = json.dumps(fields).replace("'", "''")
                safe_final = json.dumps(final_fields).replace("'", "''")
                safe_modified = ", ".join(modified).replace("'", "''") if modified else ""
                safe_notes = review_notes.replace("'", "''")
                safe_reviewer = reviewer_name.replace("'", "''")
                safe_file = st.session_state.get("source_file", "unknown").replace("'", "''")

                session.sql(f"""
                    INSERT INTO AI_PLATFORM_DEV.CURATED.POD_REVIEW_LOG
                    (REVIEW_ID, POD_ID, FILE_NAME, REVIEW_STATUS, REVIEWER, AI_CONFIDENCE,
                     FIELDS_MODIFIED, REVIEW_NOTES, RAW_AI_OUTPUT, FINAL_OUTPUT)
                    SELECT
                        '{review_id}',
                        '{pod_ref.replace("'", "''")}',
                        '{safe_file}',
                        '{review_status}',
                        '{safe_reviewer}',
                        0.85,
                        '{safe_modified}',
                        '{safe_notes}',
                        PARSE_JSON('{safe_ai}'),
                        PARSE_JSON('{safe_final}')
                """).collect()

                if approve:
                    has_exc = bool(exception_notes.strip())
                    has_dmg = bool(damage_description.strip())
                    sig_bool = "TRUE" if signature_present == "Yes" else "FALSE"
                    partial_bool = "TRUE" if partial_delivery == "Yes" else "FALSE"
                    safe_exc = exception_notes.replace("'", "''")
                    safe_dmg = damage_description.replace("'", "''")

                    session.sql(f"""
                        INSERT INTO AI_PLATFORM_DEV.CURATED.POD_FACT
                        (POD_ID, SHIPMENT_ID, ORDER_ID, CARRIER_ID, DELIVERY_DATE,
                         SIGNED_BY, RECEIVER_NAME, DELIVERY_ADDRESS, DELIVERY_CITY,
                         DELIVERY_STATE, POD_STATUS, EXCEPTION_FLAG, EXCEPTION_NOTES,
                         SIGNATURE_PRESENT, DAMAGE_REPORTED, DAMAGE_DESCRIPTION,
                         PARTIAL_DELIVERY, PACKAGES_RECEIVED, PACKAGES_EXPECTED,
                         POD_FILE_NAME, POD_TEXT_CONTENT, EXTRACTION_CONFIDENCE)
                        VALUES (
                            '{pod_ref.replace("'", "''")}',
                            '{shipment_id.replace("'", "''")}',
                            '{order_id.replace("'", "''")}',
                            '{carrier_name.replace("'", "''")}',
                            TRY_TO_DATE('{delivery_date}'),
                            '{signed_by.replace("'", "''")}',
                            '{receiver_name.replace("'", "''")}',
                            '{delivery_address.replace("'", "''")}',
                            '{delivery_city.replace("'", "''")}',
                            '{delivery_state.replace("'", "''")}',
                            '{delivery_status}',
                            {str(has_exc).upper()},
                            '{safe_exc}',
                            {sig_bool},
                            {str(has_dmg).upper()},
                            '{safe_dmg}',
                            {partial_bool},
                            TRY_TO_NUMBER('{pkgs_received}'),
                            TRY_TO_NUMBER('{pkgs_expected}'),
                            '{safe_file}',
                            NULL,
                            0.85
                        )
                    """).collect()

                    st.success(
                        f"✅ **Approved and saved** to POD_FACT (Review ID: `{review_id}`)"
                        + (f" — {len(modified)} field(s) corrected: {', '.join(modified)}" if modified else " — no corrections needed")
                    )
                    st.cache_data.clear()
                elif reject:
                    st.error(f"❌ **Rejected** (Review ID: `{review_id}`). Record NOT saved to POD_FACT.")
                else:
                    st.warning(f"🔶 **Flagged for re-review** (Review ID: `{review_id}`). Record NOT saved to POD_FACT.")

                del st.session_state["extracted_fields"]
                del st.session_state["raw_text"]

            if "extracted_fields" in st.session_state:
                with st.expander("📃 Raw extracted text"):
                    st.code(st.session_state["raw_text"][:5000], language="markdown")
                with st.expander("🔧 Raw AI JSON output"):
                    st.json(st.session_state["extracted_fields"])

        elif not process_btn and "extracted_fields" not in st.session_state:
            st.info(
                "Upload a POD document and click **Process document** to extract "
                "structured data using Cortex AI."
            )


with tab_review:
    st.subheader("📋 Review queue")

    filter_col1, filter_col2 = st.columns([1, 3])
    status_filter = filter_col1.selectbox(
        "Filter by status",
        ["All", "APPROVED", "REJECTED", "FLAGGED"],
    )

    where_clause = ""
    if status_filter != "All":
        where_clause = f"WHERE REVIEW_STATUS = '{status_filter}'"

    @st.cache_data(ttl=30)
    def load_reviews(where):
        return conn.query(f"""
            SELECT
                REVIEW_ID, POD_ID, FILE_NAME, REVIEW_STATUS, REVIEWER,
                REVIEW_TIMESTAMP, AI_CONFIDENCE, FIELDS_MODIFIED, REVIEW_NOTES
            FROM AI_PLATFORM_DEV.CURATED.POD_REVIEW_LOG
            {where}
            ORDER BY REVIEW_TIMESTAMP DESC
            LIMIT 100
        """)

    reviews = load_reviews(where_clause)

    if reviews.empty:
        st.info("No reviews yet. Process a POD document in the first tab to get started.")
    else:
        approved = len(reviews[reviews["REVIEW_STATUS"] == "APPROVED"])
        rejected = len(reviews[reviews["REVIEW_STATUS"] == "REJECTED"])
        flagged = len(reviews[reviews["REVIEW_STATUS"] == "FLAGGED"])
        total = len(reviews)

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total reviews", total)
        m2.metric("Approved", approved)
        m3.metric("Rejected", rejected)
        m4.metric("Flagged", flagged)
        m5.metric("Auto-accept rate", f"{round(100 * approved / total, 1)}%" if total > 0 else "—")

        corrections = reviews[reviews["FIELDS_MODIFIED"].notna() & (reviews["FIELDS_MODIFIED"] != "")]
        if not corrections.empty:
            st.caption(f"📝 {len(corrections)} of {approved} approved PODs had manual corrections")

        st.dataframe(
            reviews,
            hide_index=True,
            use_container_width=True,
            column_config={
                "REVIEW_STATUS": st.column_config.TextColumn("Status"),
                "AI_CONFIDENCE": st.column_config.ProgressColumn(
                    "AI confidence", min_value=0, max_value=1, format="%.0f%%"
                ),
                "REVIEW_TIMESTAMP": st.column_config.DatetimeColumn("Reviewed at", format="YYYY-MM-DD HH:mm"),
            },
        )


with tab_dashboard:

    @st.cache_data(ttl=300)
    def load_pod_summary():
        return conn.query("""
            SELECT
                COUNT(*) AS TOTAL_PODS,
                SUM(CASE WHEN EXCEPTION_FLAG THEN 1 ELSE 0 END) AS WITH_EXCEPTIONS,
                SUM(CASE WHEN NOT SIGNATURE_PRESENT THEN 1 ELSE 0 END) AS MISSING_SIGS,
                SUM(CASE WHEN DAMAGE_REPORTED THEN 1 ELSE 0 END) AS WITH_DAMAGE,
                SUM(CASE WHEN PARTIAL_DELIVERY THEN 1 ELSE 0 END) AS PARTIAL_DELS,
                ROUND(100.0 * SUM(CASE WHEN POD_STATUS = 'Delivered - Clean' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 1) AS CLEAN_PCT
            FROM AI_PLATFORM_DEV.CURATED.POD_FACT
        """)

    @st.cache_data(ttl=300)
    def load_exceptions_by_carrier():
        return conn.query("""
            SELECT
                c.CARRIER_NAME,
                COUNT(*) AS TOTAL_PODS,
                SUM(CASE WHEN p.EXCEPTION_FLAG THEN 1 ELSE 0 END) AS EXCEPTIONS,
                ROUND(100.0 * SUM(CASE WHEN p.EXCEPTION_FLAG THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 1) AS EXCEPTION_PCT
            FROM AI_PLATFORM_DEV.CURATED.POD_FACT p
            LEFT JOIN AI_PLATFORM_DEV.CURATED.DIM_CARRIERS c ON p.CARRIER_ID = c.CARRIER_ID
            GROUP BY c.CARRIER_NAME
            ORDER BY EXCEPTION_PCT DESC
        """)

    @st.cache_data(ttl=300)
    def load_exceptions_by_customer():
        return conn.query("""
            SELECT
                c.CUSTOMER_NAME,
                c.CUSTOMER_SEGMENT,
                COUNT(*) AS TOTAL_PODS,
                SUM(CASE WHEN p.EXCEPTION_FLAG THEN 1 ELSE 0 END) AS EXCEPTIONS,
                SUM(CASE WHEN p.DAMAGE_REPORTED THEN 1 ELSE 0 END) AS DAMAGES,
                SUM(CASE WHEN NOT p.SIGNATURE_PRESENT THEN 1 ELSE 0 END) AS MISSING_SIGS
            FROM AI_PLATFORM_DEV.CURATED.POD_FACT p
            LEFT JOIN AI_PLATFORM_DEV.CURATED.DIM_CUSTOMERS c ON p.CUSTOMER_ID = c.CUSTOMER_ID
            GROUP BY c.CUSTOMER_NAME, c.CUSTOMER_SEGMENT
            ORDER BY EXCEPTIONS DESC
        """)

    @st.cache_data(ttl=300)
    def load_pod_status_dist():
        return conn.query("""
            SELECT POD_STATUS, COUNT(*) AS CNT
            FROM AI_PLATFORM_DEV.CURATED.POD_FACT
            GROUP BY POD_STATUS
            ORDER BY CNT DESC
        """)

    @st.cache_data(ttl=300)
    def load_monthly_trend():
        return conn.query("""
            SELECT
                DATE_TRUNC('month', DELIVERY_DATE)::DATE AS MONTH,
                COUNT(*) AS TOTAL_PODS,
                SUM(CASE WHEN EXCEPTION_FLAG THEN 1 ELSE 0 END) AS EXCEPTIONS,
                SUM(CASE WHEN DAMAGE_REPORTED THEN 1 ELSE 0 END) AS DAMAGES
            FROM AI_PLATFORM_DEV.CURATED.POD_FACT
            GROUP BY MONTH
            ORDER BY MONTH
        """)

    @st.cache_data(ttl=300)
    def load_recent_exceptions():
        return conn.query("""
            SELECT
                p.POD_ID, p.SHIPMENT_ID, p.DELIVERY_DATE, p.POD_STATUS,
                c.CUSTOMER_NAME, cr.CARRIER_NAME,
                p.EXCEPTION_NOTES, p.DAMAGE_DESCRIPTION,
                p.SIGNATURE_PRESENT
            FROM AI_PLATFORM_DEV.CURATED.POD_FACT p
            LEFT JOIN AI_PLATFORM_DEV.CURATED.DIM_CUSTOMERS c ON p.CUSTOMER_ID = c.CUSTOMER_ID
            LEFT JOIN AI_PLATFORM_DEV.CURATED.DIM_CARRIERS cr ON p.CARRIER_ID = cr.CARRIER_ID
            WHERE p.EXCEPTION_FLAG = TRUE
            ORDER BY p.DELIVERY_DATE DESC
            LIMIT 50
        """)

    summary = load_pod_summary()
    s = summary.iloc[0]

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total PODs", f"{int(s['TOTAL_PODS']):,}")
    k2.metric("Clean delivery %", f"{s['CLEAN_PCT']}%")
    k3.metric("With exceptions", f"{int(s['WITH_EXCEPTIONS']):,}")
    k4.metric("Missing signatures", f"{int(s['MISSING_SIGS']):,}")
    k5.metric("Damage reported", f"{int(s['WITH_DAMAGE']):,}")
    k6.metric("Partial deliveries", f"{int(s['PARTIAL_DELS']):,}")

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("POD status distribution")
        status_df = load_pod_status_dist()
        st.bar_chart(status_df.set_index("POD_STATUS"))

    with col_right:
        st.subheader("Monthly POD trend")
        trend_df = load_monthly_trend()
        if not trend_df.empty:
            st.line_chart(trend_df.set_index("MONTH"))

    col_carrier, col_customer = st.columns(2)

    with col_carrier:
        st.subheader("Exception rate by carrier")
        carrier_df = load_exceptions_by_carrier()
        st.dataframe(
            carrier_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "EXCEPTION_PCT": st.column_config.ProgressColumn(
                    "Exception %", min_value=0, max_value=100, format="%.1f%%"
                ),
            },
        )

    with col_customer:
        st.subheader("Exceptions by customer")
        customer_df = load_exceptions_by_customer()
        st.dataframe(customer_df, hide_index=True, use_container_width=True)

    st.subheader("Recent POD exceptions")
    exceptions_df = load_recent_exceptions()
    st.dataframe(
        exceptions_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "SIGNATURE_PRESENT": st.column_config.CheckboxColumn("Signed"),
        },
    )
