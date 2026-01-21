"""Advanced Analytics page for MAUDE Analyzer."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, datetime, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, CHART_COLORS, MANUFACTURER_COLORS, EVENT_TYPES
from src.database import get_connection
from src.analysis import (
    get_filter_options,
    detect_signals,
    SignalSeverity,
    get_summary_statistics,
    compare_manufacturers,
    rank_manufacturers_by_metric,
    get_term_frequency,
    get_keyword_trends,
    search_narratives,
    ADVERSE_EVENT_TERMS,
    generate_html_report,
    ReportConfig,
)


def render_analytics():
    """Render the advanced analytics page."""
    st.markdown("Advanced analysis tools including signal detection, text analysis, and reports.")

    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        return

    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "Safety Signals",
        "Text Analysis",
        "Statistical Comparison",
        "Generate Report",
    ])

    with tab1:
        render_signals_tab()

    with tab2:
        render_text_analysis_tab()

    with tab3:
        render_comparison_tab()

    with tab4:
        render_report_tab()


def render_signals_tab():
    """Render the safety signals detection tab."""
    st.subheader("Safety Signal Detection")
    st.markdown(
        "Automatically detect unusual patterns in MDR data that may indicate emerging safety issues."
    )

    # Run detection
    if st.button("Run Signal Detection", type="primary"):
        with st.spinner("Analyzing data for safety signals..."):
            try:
                result = detect_signals()

                if not result.signals:
                    st.success("No significant safety signals detected.")
                    return

                st.info(f"Detected {len(result.signals)} safety signal(s)")

                # Display signals by severity
                for signal in result.signals:
                    severity_colors = {
                        SignalSeverity.CRITICAL: "red",
                        SignalSeverity.HIGH: "orange",
                        SignalSeverity.MEDIUM: "yellow",
                        SignalSeverity.LOW: "blue",
                    }
                    color = severity_colors.get(signal.severity, "gray")

                    with st.container():
                        col1, col2 = st.columns([1, 4])

                        with col1:
                            if signal.severity == SignalSeverity.CRITICAL:
                                st.error(f"**{signal.severity.value.upper()}**")
                            elif signal.severity == SignalSeverity.HIGH:
                                st.warning(f"**{signal.severity.value.upper()}**")
                            elif signal.severity == SignalSeverity.MEDIUM:
                                st.info(f"**{signal.severity.value.upper()}**")
                            else:
                                st.info(f"**{signal.severity.value.upper()}**")

                        with col2:
                            st.markdown(f"**{signal.title}**")
                            st.markdown(signal.description)

                            if signal.percent_change:
                                st.caption(f"Change: {signal.percent_change:.1f}%")
                            if signal.z_score:
                                st.caption(f"Z-score: {signal.z_score:.2f}")

                        st.divider()

            except Exception as e:
                st.error(f"Error detecting signals: {e}")

    # Signal explanation
    with st.expander("About Signal Detection"):
        st.markdown("""
        **Signal Types Detected:**
        - **Volume Spikes**: Unusual increases in MDR volume compared to historical baseline
        - **Death Rate Changes**: Significant changes in death rates
        - **Manufacturer Trends**: Year-over-year changes in MDR patterns

        **Severity Levels:**
        - **Critical**: Requires immediate attention (z-score > 4 or death rate > 5%)
        - **High**: Significant concern (z-score > 3)
        - **Medium**: Notable pattern (z-score > 2)
        - **Low**: Minor deviation (z-score > threshold)

        *Signals are based on statistical analysis comparing recent data to historical baselines.*
        """)


def render_text_analysis_tab():
    """Render the text/narrative analysis tab."""
    st.subheader("Narrative Text Analysis")

    try:
        with get_connection() as conn:
            filter_options = get_filter_options(conn)
    except Exception as e:
        st.error(f"Error loading filter options: {e}")
        return

    # Analysis type selection
    analysis_type = st.radio(
        "Analysis Type",
        options=["Term Frequency", "Keyword Extraction", "Narrative Search"],
        horizontal=True,
    )

    st.divider()

    if analysis_type == "Term Frequency":
        render_term_frequency_analysis(filter_options)
    elif analysis_type == "Keyword Extraction":
        render_keyword_analysis(filter_options)
    else:
        render_narrative_search(filter_options)


def render_term_frequency_analysis(filter_options: dict):
    """Render term frequency analysis."""
    st.markdown("Analyze frequency of adverse event terms in MDR narratives.")

    col1, col2 = st.columns(2)

    with col1:
        category = st.selectbox(
            "Term Category",
            options=["All Categories"] + list(ADVERSE_EVENT_TERMS.keys()),
        )

    with col2:
        manufacturers = st.multiselect(
            "Filter by Manufacturer",
            options=filter_options.get("manufacturers", []),
        )

    if st.button("Analyze Terms"):
        with st.spinner("Analyzing term frequency..."):
            try:
                with get_connection() as conn:
                    df = get_term_frequency(
                        term_category=category if category != "All Categories" else None,
                        manufacturers=manufacturers if manufacturers else None,
                        conn=conn,
                    )

                if df.empty:
                    st.info("No term matches found.")
                    return

                # Display results
                st.subheader("Term Frequency Results")

                # Bar chart
                fig = px.bar(
                    df.head(20),
                    x="count",
                    y="term",
                    orientation="h",
                    color="category",
                    title="Top Terms by Frequency",
                    labels={"count": "MDRs Mentioning Term", "term": "Term"},
                )
                fig.update_layout(height=500, yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(fig, use_container_width=True)

                # Data table
                with st.expander("View Data Table"):
                    st.dataframe(df, use_container_width=True, hide_index=True)

            except Exception as e:
                st.error(f"Error analyzing terms: {e}")


def render_keyword_analysis(filter_options: dict):
    """Render keyword extraction analysis."""
    st.markdown("Extract most frequent keywords from MDR narratives.")

    col1, col2 = st.columns(2)

    with col1:
        n_keywords = st.slider("Number of Keywords", 10, 50, 25)

    with col2:
        manufacturers = st.multiselect(
            "Filter by Manufacturer",
            options=filter_options.get("manufacturers", []),
            key="keyword_mfr",
        )

    if st.button("Extract Keywords"):
        with st.spinner("Extracting keywords..."):
            try:
                with get_connection() as conn:
                    df = get_keyword_trends(
                        n_keywords=n_keywords,
                        manufacturers=manufacturers if manufacturers else None,
                        conn=conn,
                    )

                if df.empty:
                    st.info("No keywords found.")
                    return

                # Word cloud-style visualization using bar chart
                fig = px.bar(
                    df,
                    x="keyword",
                    y="count",
                    title="Top Keywords in MDR Narratives",
                    labels={"count": "Frequency", "keyword": "Keyword"},
                )
                fig.update_layout(xaxis_tickangle=-45, height=400)
                st.plotly_chart(fig, use_container_width=True)

                # Data table
                st.dataframe(df, use_container_width=True, hide_index=True)

            except Exception as e:
                st.error(f"Error extracting keywords: {e}")


def render_narrative_search(filter_options: dict):
    """Render narrative search functionality."""
    st.markdown("Search MDR narratives for specific terms or phrases.")

    search_terms = st.text_input(
        "Search Terms (comma-separated)",
        placeholder="e.g., migration, lead fracture, battery"
    )

    col1, col2 = st.columns(2)

    with col1:
        manufacturers = st.multiselect(
            "Filter by Manufacturer",
            options=filter_options.get("manufacturers", []),
            key="search_mfr",
        )

    with col2:
        limit = st.number_input("Max Results", 10, 500, 50)

    if st.button("Search Narratives") and search_terms:
        terms = [t.strip() for t in search_terms.split(",")]

        with st.spinner("Searching narratives..."):
            try:
                with get_connection() as conn:
                    df = search_narratives(
                        search_terms=terms,
                        manufacturers=manufacturers if manufacturers else None,
                        limit=limit,
                        conn=conn,
                    )

                if df.empty:
                    st.info("No matching narratives found.")
                    return

                st.success(f"Found {len(df)} matching MDRs")

                # Display results
                for _, row in df.head(20).iterrows():
                    with st.expander(
                        f"MDR {row['mdr_report_key']} - {row['manufacturer_clean']} ({row['date_received']})"
                    ):
                        st.markdown(f"**Event Type:** {EVENT_TYPES.get(row['event_type'], row['event_type'])}")
                        st.markdown(f"**Product Code:** {row['product_code']}")
                        st.divider()
                        # Highlight search terms in text
                        text = row["text_content"] or ""
                        for term in terms:
                            text = text.replace(term, f"**{term}**")
                            text = text.replace(term.upper(), f"**{term.upper()}**")
                            text = text.replace(term.capitalize(), f"**{term.capitalize()}**")
                        st.markdown(text[:2000] + ("..." if len(text) > 2000 else ""))

            except Exception as e:
                st.error(f"Error searching narratives: {e}")


def render_comparison_tab():
    """Render the statistical comparison tab."""
    st.subheader("Statistical Comparison")

    try:
        with get_connection() as conn:
            filter_options = get_filter_options(conn)
    except Exception as e:
        st.error(f"Error loading filter options: {e}")
        return

    comparison_type = st.radio(
        "Comparison Type",
        options=["Manufacturer vs Manufacturer", "Manufacturer Rankings"],
        horizontal=True,
    )

    st.divider()

    if comparison_type == "Manufacturer vs Manufacturer":
        render_head_to_head_comparison(filter_options)
    else:
        render_rankings(filter_options)


def render_head_to_head_comparison(filter_options: dict):
    """Render head-to-head manufacturer comparison."""
    manufacturers = filter_options.get("manufacturers", [])

    col1, col2 = st.columns(2)

    with col1:
        mfr_a = st.selectbox("Manufacturer A", options=manufacturers, key="mfr_a")

    with col2:
        mfr_b = st.selectbox(
            "Manufacturer B",
            options=[m for m in manufacturers if m != mfr_a],
            key="mfr_b",
        )

    if st.button("Compare") and mfr_a and mfr_b:
        with st.spinner("Comparing manufacturers..."):
            try:
                with get_connection() as conn:
                    results = compare_manufacturers(mfr_a, mfr_b, conn=conn)

                if not results:
                    st.warning("No data available for comparison.")
                    return

                st.subheader(f"{mfr_a} vs {mfr_b}")

                # Create comparison table
                comparison_data = []
                for metric_name, result in results.items():
                    comparison_data.append({
                        "Metric": result.metric,
                        mfr_a: f"{result.value_a:.2f}",
                        mfr_b: f"{result.value_b:.2f}",
                        "Difference": f"{result.difference:+.2f}",
                        "Significant": "Yes" if result.significant else "No",
                    })

                df = pd.DataFrame(comparison_data)
                st.dataframe(df, use_container_width=True, hide_index=True)

                # Visual comparison
                fig = go.Figure()

                metrics = [r.metric for r in results.values()]
                values_a = [r.value_a for r in results.values()]
                values_b = [r.value_b for r in results.values()]

                fig.add_trace(go.Bar(
                    name=mfr_a,
                    x=metrics,
                    y=values_a,
                    marker_color=MANUFACTURER_COLORS.get(mfr_a, CHART_COLORS["primary"]),
                ))

                fig.add_trace(go.Bar(
                    name=mfr_b,
                    x=metrics,
                    y=values_b,
                    marker_color=MANUFACTURER_COLORS.get(mfr_b, CHART_COLORS["secondary"]),
                ))

                fig.update_layout(
                    barmode="group",
                    title="Metric Comparison",
                    height=400,
                )

                st.plotly_chart(fig, use_container_width=True)

                # Statistical notes
                with st.expander("Statistical Notes"):
                    st.markdown("""
                    **Significance Testing:**
                    - Rate comparisons use a two-proportion z-test
                    - Results marked "Significant" have p-value < 0.05
                    - This indicates the difference is unlikely due to chance alone
                    """)

            except Exception as e:
                st.error(f"Error comparing manufacturers: {e}")


def render_rankings(filter_options: dict):
    """Render manufacturer rankings."""
    col1, col2 = st.columns(2)

    with col1:
        metric = st.selectbox(
            "Rank By",
            options=["death_rate", "injury_rate", "malfunction_rate", "total_mdrs"],
            format_func=lambda x: x.replace("_", " ").title(),
        )

    with col2:
        min_mdrs = st.number_input("Minimum MDRs", 1, 1000, 10)

    if st.button("Show Rankings"):
        with st.spinner("Calculating rankings..."):
            try:
                with get_connection() as conn:
                    df = rank_manufacturers_by_metric(
                        metric=metric,
                        min_mdrs=min_mdrs,
                        conn=conn,
                    )

                if df.empty:
                    st.info("No manufacturers meet the minimum MDR threshold.")
                    return

                st.subheader(f"Manufacturers Ranked by {metric.replace('_', ' ').title()}")

                # Display table
                display_cols = ["rank", "manufacturer_clean", "total_mdrs", metric]
                st.dataframe(
                    df[display_cols].head(20),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "rank": "Rank",
                        "manufacturer_clean": "Manufacturer",
                        "total_mdrs": "Total MDRs",
                        metric: metric.replace("_", " ").title(),
                    }
                )

                # Bar chart
                fig = px.bar(
                    df.head(15),
                    x="manufacturer_clean",
                    y=metric,
                    color=metric,
                    color_continuous_scale="Reds" if "death" in metric else "Blues",
                    title=f"Top 15 by {metric.replace('_', ' ').title()}",
                )
                fig.update_layout(xaxis_tickangle=-45, height=400)
                st.plotly_chart(fig, use_container_width=True)

            except Exception as e:
                st.error(f"Error calculating rankings: {e}")


def render_report_tab():
    """Render the report generation tab."""
    st.subheader("Generate Report")
    st.markdown("Create a comprehensive HTML report that can be printed or saved as PDF.")

    try:
        with get_connection() as conn:
            filter_options = get_filter_options(conn)
    except Exception as e:
        st.error(f"Error loading filter options: {e}")
        return

    # Report options
    col1, col2 = st.columns(2)

    with col1:
        title = st.text_input("Report Title", value="MAUDE Analysis Report")
        subtitle = st.text_input("Subtitle (optional)", value="")

    with col2:
        manufacturers = st.multiselect(
            "Filter by Manufacturer",
            options=filter_options.get("manufacturers", []),
            key="report_mfr",
        )

    # Sections to include
    st.markdown("**Sections to Include:**")
    col1, col2 = st.columns(2)

    with col1:
        include_summary = st.checkbox("Executive Summary", value=True)
        include_trends = st.checkbox("Trends Analysis", value=True)

    with col2:
        include_comparison = st.checkbox("Manufacturer Comparison", value=True)
        include_signals = st.checkbox("Safety Signals", value=True)

    if st.button("Generate Report", type="primary"):
        with st.spinner("Generating report..."):
            try:
                report_config = ReportConfig(
                    title=title,
                    subtitle=subtitle if subtitle else None,
                    include_executive_summary=include_summary,
                    include_trends=include_trends,
                    include_manufacturer_comparison=include_comparison,
                    include_signals=include_signals,
                    manufacturers=manufacturers if manufacturers else None,
                )

                with get_connection() as conn:
                    html_content = generate_html_report(report_config, conn)

                # Provide download button
                st.download_button(
                    label="Download HTML Report",
                    data=html_content,
                    file_name=f"maude_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                    mime="text/html",
                )

                st.success("Report generated successfully!")

                # Preview
                with st.expander("Preview Report"):
                    st.components.v1.html(html_content, height=600, scrolling=True)

            except Exception as e:
                st.error(f"Error generating report: {e}")
