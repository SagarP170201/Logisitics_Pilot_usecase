import streamlit as st
import pandas as pd
import json
import tempfile
import os

st.set_page_config(
    page_title="POD Document Processing",
    page_icon="🚚",
    layout="wide",
)

conn = st.connection("snowflake")
session = conn.session()

st.title("🚚 POD document processing")
st.caption("Use Case 1 — Upload a POD PDF, extract structured data with Cortex AI, and view logistics analytics")

tab_upload, tab_dashboard = st.tabs([
    "📄 Process POD document",
    "📊 POD analytics dashboard",
])

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
                status.update(label="Processing complete", state="complete")

            st.subheader("Extracted fields")

            has_exception = bool(fields.get("exception_notes"))
            has_damage = bool(fields.get("damage_description"))
            sig_present = str(fields.get("signature_present", "")).lower() in (
                "yes", "true", "1",
            )

            status_badges = []
            delivery_status = fields.get("delivery_status", "Unknown")
            if "Clean" in str(delivery_status):
                status_badges.append(f"🟢 **{delivery_status}**")
            else:
                status_badges.append(f"🟠 **{delivery_status}**")
            status_badges.append(
                "🔴 **Exception**" if has_exception else "🟢 **No exceptions**"
            )
            status_badges.append(
                "🟢 **Signed**" if sig_present else "🔴 **Missing signature**"
            )
            status_badges.append(
                "🔴 **Damage reported**" if has_damage else "🟢 **No damage**"
            )
            st.markdown(" &nbsp;&nbsp;|&nbsp;&nbsp; ".join(status_badges))

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("POD reference", fields.get("pod_reference", "—"))
            c2.metric("Shipment ID", fields.get("shipment_id", "—"))
            c3.metric("Delivery date", fields.get("delivery_date", "—"))
            c4.metric(
                "Packages",
                f"{fields.get('packages_received', '?')} / {fields.get('packages_expected', '?')}",
            )

            c5, c6, c7, c8 = st.columns(4)
            c5.metric("Customer", fields.get("customer_name", "—"))
            c6.metric("Carrier", fields.get("carrier_name", "—"))
            c7.metric("Signed by", fields.get("signed_by", "—"))
            c8.metric("Receiver", fields.get("receiver_name", "—"))

            c9, c10 = st.columns(2)
            c9.metric("City", fields.get("delivery_city", "—"))
            c10.metric("State", fields.get("delivery_state", "—"))

            if fields.get("delivery_address"):
                st.caption(f"📍 {fields['delivery_address']}")

            if has_exception:
                st.warning(f"⚠️ **Exception notes:** {fields['exception_notes']}")

            if has_damage:
                st.error(f"💥 **Damage:** {fields['damage_description']}")

            with st.expander("📃 Raw extracted text"):
                st.code(raw_text[:5000], language="markdown")

            with st.expander("🔧 Full JSON output"):
                st.json(fields)

        elif not process_btn:
            st.info(
                "Upload a POD document and click **Process document** to extract "
                "structured data using Cortex AI."
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
