"""Filter preset management component for MAUDE Analyzer."""

import streamlit as st
from typing import Optional

from .filter_state import (
    get_filter_state,
    get_filter_presets,
    save_preset,
    delete_preset,
    apply_preset,
    get_default_presets,
)


def render_preset_selector(
    key: str = "preset_selector",
    on_change_callback: Optional[callable] = None,
) -> Optional[str]:
    """
    Render a preset selector dropdown.

    Args:
        key: Unique key for the Streamlit widget.
        on_change_callback: Optional callback when preset changes.

    Returns:
        Name of selected preset, or None.
    """
    presets = get_filter_presets()
    preset_names = list(presets.keys())

    selected = st.selectbox(
        "Load Preset",
        options=["(None)"] + preset_names,
        index=0,
        key=key,
        help="Load a saved filter configuration",
    )

    if selected != "(None)":
        apply_preset(selected)
        if on_change_callback:
            on_change_callback()
        return selected

    return None


def render_preset_manager(key: str = "preset_manager") -> None:
    """
    Render a full preset management UI.

    Args:
        key: Unique key for the Streamlit widget.
    """
    st.markdown("**Filter Presets**")

    presets = get_filter_presets()
    default_presets = get_default_presets()

    # Load preset section
    col1, col2 = st.columns([2, 1])

    with col1:
        preset_names = list(presets.keys())
        selected = st.selectbox(
            "Select Preset",
            options=preset_names,
            index=0 if preset_names else None,
            key=f"{key}_select",
        )

    with col2:
        st.write("")  # Spacer
        st.write("")
        if st.button("Apply", key=f"{key}_apply", disabled=not selected):
            apply_preset(selected)
            st.success(f"Applied preset: {selected}")
            st.rerun()

    st.divider()

    # Save new preset
    st.markdown("**Save Current Filters as Preset**")

    col1, col2 = st.columns([2, 1])

    with col1:
        new_name = st.text_input(
            "Preset Name",
            placeholder="Enter preset name...",
            key=f"{key}_new_name",
        )

    with col2:
        st.write("")  # Spacer
        st.write("")
        if st.button("Save", key=f"{key}_save", disabled=not new_name):
            state = get_filter_state()
            save_preset(new_name, state)
            st.success(f"Saved preset: {new_name}")
            st.rerun()

    # Delete preset (only user-created ones)
    user_presets = [name for name in presets.keys() if name not in default_presets]

    if user_presets:
        st.divider()
        st.markdown("**Delete Preset**")

        col1, col2 = st.columns([2, 1])

        with col1:
            to_delete = st.selectbox(
                "Select preset to delete",
                options=user_presets,
                key=f"{key}_delete_select",
            )

        with col2:
            st.write("")
            st.write("")
            if st.button("Delete", key=f"{key}_delete", type="secondary"):
                if delete_preset(to_delete):
                    st.success(f"Deleted preset: {to_delete}")
                    st.rerun()
                else:
                    st.warning("Cannot delete default presets")


def render_preset_chips(key: str = "preset_chips") -> Optional[str]:
    """
    Render presets as clickable chips/buttons for quick access.

    Args:
        key: Unique key for the Streamlit widget.

    Returns:
        Name of applied preset, or None.
    """
    presets = get_filter_presets()

    if not presets:
        return None

    st.caption("Quick presets:")

    # Create columns for preset buttons
    cols = st.columns(min(len(presets), 4))

    for i, (name, preset) in enumerate(presets.items()):
        col_idx = i % len(cols)
        with cols[col_idx]:
            # Show preset info as button
            if preset.product_codes:
                label = f"{name} ({len(preset.product_codes)} products)"
            elif preset.manufacturers:
                label = f"{name} ({len(preset.manufacturers)} manufacturers)"
            else:
                label = name

            if st.button(label, key=f"{key}_{name}", use_container_width=True):
                apply_preset(name)
                st.rerun()

    return None
