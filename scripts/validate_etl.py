#!/usr/bin/env python3
"""
ETL Validation Script — Clinical Data Health Reporter

Validates wide-to-long ETL output against a metadata-driven mapping configuration.
Produces a structured "Data Health Report" for every entity found in the config.

Checks performed per entity
----------------------------
1. Completeness  : Non-null wide-table values for entity columns == long-table row count.
2. Column Coverage: Every source_label defined in the config appears in the long
                    table's {prefix}_source_text field.
3. Participant Distribution: Prevalence frequency table (top-N codes) + rare-code summary.

Directory layout expected
--------------------------
    project_root/
    ├── data/
    │   ├── source/
    │   │   └── master_wide.csv          ← --wide
    │   └── mapped/
    │       └── STUDY/
    │           ├── phenotype.csv        ← auto-discovered via --long-dir
    │           ├── medication.csv
    │           └── ...
    └── mapping_config.tsv               ← --config

mapping_config.tsv columns (tab-separated)
-------------------------------------------
    source_variable | source_label | target_code | target_term | mapped_in_entities

    source_variable  : actual column name in the wide table
    source_label     : human-readable description of that column
    mapped_in_entities may contain one or more comma-separated entity names,
    e.g. "phenotype" or "phenotype, measurement".

Column prefix rules (auto-derived, no configuration needed)
-----------------------------------------------------------
    medication  →  drug    (drug_code, drug_term, drug_source_text)
    <any other> →  <entity> (<entity>_code, <entity>_term, <entity>_source_text)

Usage
-----
    # Validate all entities
    python scripts/validate_etl.py \\
        --wide    data/source/master_wide.csv \\
        --long-dir data/mapped/STUDY \\
        --config  mapping_config.tsv

    # Validate specific entities only
    python scripts/validate_etl.py \\
        --wide    data/source/master_wide.csv \\
        --long-dir data/mapped/STUDY \\
        --config  mapping_config.tsv \\
        --entity  phenotype medication \\
        --wide-participant-id id_hostseq \
        --long-participant-id submitter_participant_id \
        --long-suffix .csv \
        --rare-threshold 1 \\
        --top-n 5 \\
        --output  reports/
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LONG_TABLE_SUFFIX = ".csv"  # default; overridden per-instance via long_suffix
SEPARATOR = "=" * 72

# TSV columns expected in mapping_config
CONFIG_COLS = {"source_variable", "source_label", "target_code", "target_term", "mapped_in_entities"}

# Entities whose long-table column prefix differs from their entity name.
# All other entities use the entity name itself as the prefix.
ENTITY_PREFIX_OVERRIDES: Dict[str, str] = {
    "medication": "drug",
}


def get_entity_prefix(entity: str) -> str:
    """Return the column prefix for *entity* (e.g. 'medication' → 'drug')."""
    key = entity.lower().strip()
    return ENTITY_PREFIX_OVERRIDES.get(key, key)


# ---------------------------------------------------------------------------
# ETLValidator
# ---------------------------------------------------------------------------
class ETLValidator:
    """
    Metadata-driven ETL validator for wide-to-long clinical data pipelines.

    Parameters
    ----------
    wide_pid_col : str
        Name of the participant identifier column in the wide table.
    long_pid_col : str
        Name of the participant identifier column in the long tables.
    rare_threshold : int
        Maximum number of unique participants for a code to be labelled
        "rare" (inclusive).  Default: 1.
    top_n : int
        Number of top codes to display in the participant distribution table.
        Default: 5.
    long_suffix : str
        File suffix appended to the entity name to form the long-table
        filename (default: ".csv").
    skipped_values : list of str, optional
        Wide-table cell values (matched as strings) that do **not** generate
        a long-table row and should therefore be excluded from expected-row
        counts in Checks 1, 2, and 4.  For example, pass ``["0", "-1"]``
        for entities where ``0`` means "not performed" and ``-1`` means
        "unknown" — only cells with other values are counted as qualifying.
        When ``None`` (default), all non-null cells are counted.
    """

    def __init__(
        self,
        wide_pid_col: str = "participant_id",
        long_pid_col: str = "participant_id",
        rare_threshold: int = 1,
        top_n: int = 5,
        long_suffix: str = ".csv",
        skipped_values: Optional[List[str]] = None,
    ) -> None:
        self.wide_pid_col = wide_pid_col
        self.long_pid_col = long_pid_col
        self.rare_threshold = rare_threshold
        self.top_n = top_n
        self.long_suffix = long_suffix
        self.skipped_values = skipped_values

    # ------------------------------------------------------------------
    # Loaders
    # ------------------------------------------------------------------

    def load_config(self, config_path: Path) -> pd.DataFrame:
        """
        Load and validate the mapping configuration TSV.

        ``mapped_in_entities`` may contain comma-separated entity names; this
        method explodes them so every row represents a single (source_label, entity)
        pairing.  A derived ``prefix`` column is added automatically.

        Returns
        -------
        pd.DataFrame with columns:
            source_variable, source_label, target_code, target_term, entity, prefix
        """
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        sep = "\t" if config_path.suffix.lower() in {".tsv", ".tab"} else ","
        df = pd.read_csv(config_path, sep=sep, dtype=str).fillna("")

        missing = CONFIG_COLS - set(df.columns)
        if missing:
            raise ValueError(
                f"mapping_config is missing required columns: {missing}"
            )

        df = df[list(CONFIG_COLS)].copy()
        df = df[df["source_variable"].str.strip() != ""]  # drop rows with no variable name

        # Explode multi-entity cells (e.g. "phenotype, measurement")
        df["entity"] = df["mapped_in_entities"].str.split(r",\s*")
        df = df.explode("entity")
        df["entity"] = df["entity"].str.strip().str.lower()
        df = df[df["entity"] != ""]  # drop any empty tokens

        # Derive column prefix automatically
        df["prefix"] = df["entity"].map(get_entity_prefix)

        df = df.drop(columns=["mapped_in_entities"]).reset_index(drop=True)

        logger.info(
            "Config loaded: %d mappings across %d entity/entities",
            len(df),
            df["entity"].nunique(),
        )
        return df

    def load_wide_table(self, wide_path: Path) -> pd.DataFrame:
        """Load the master wide table."""
        wide_path = Path(wide_path)
        if not wide_path.exists():
            raise FileNotFoundError(f"Wide table not found: {wide_path}")

        df = pd.read_csv(wide_path, low_memory=False)
        if self.wide_pid_col not in df.columns:
            raise ValueError(
                f"Wide participant ID column '{self.wide_pid_col}' not found "
                f"in wide table. Available columns: {list(df.columns[:10])} …"
            )
        logger.info(
            "Wide table loaded: %d rows × %d columns from %s",
            len(df),
            len(df.columns),
            wide_path.name,
        )
        return df

    def load_participant_roster(self, long_dir: Path) -> Optional[pd.Series]:
        """
        Load the eligible participant list from ``participant.csv`` in *long_dir*.

        Returns a pandas Series of eligible participant IDs (using ``long_pid_col``),
        or None if the file is not found (a warning is emitted and all wide-table
        participants are treated as eligible).
        """
        roster_path = Path(long_dir) / "participant.csv"
        if not roster_path.exists():
            logger.warning(
                "participant.csv not found in %s — all wide-table participants "
                "will be treated as eligible.",
                long_dir,
            )
            return None

        df = pd.read_csv(roster_path, dtype=str)
        if self.long_pid_col not in df.columns:
            logger.warning(
                "Participant ID column '%s' not found in participant.csv "
                "(columns: %s) — skipping eligibility filter.",
                self.long_pid_col,
                list(df.columns),
            )
            return None

        ids = df[self.long_pid_col].dropna().unique()
        logger.info(
            "Participant roster loaded: %d eligible participants from %s",
            len(ids),
            roster_path.name,
        )
        return pd.Series(ids)

    def load_long_table(self, long_dir: Path, entity: str) -> Optional[pd.DataFrame]:
        """
        Discover and load the long table for *entity*.

        The filename pattern is ``{entity}{self.long_suffix}``.
        Returns None if the file does not exist (reported as a warning).
        """
        long_path = Path(long_dir) / f"{entity}{self.long_suffix}"
        if not long_path.exists():
            logger.warning("Long table not found for entity '%s': %s", entity, long_path)
            return None

        df = pd.read_csv(long_path, low_memory=False)
        logger.info(
            "Long table loaded for '%s': %d rows from %s",
            entity,
            len(df),
            long_path.name,
        )
        return df

    # ------------------------------------------------------------------
    # Check 1 — Completeness
    # ------------------------------------------------------------------

    def check_completeness(
        self,
        entity: str,
        entity_config: pd.DataFrame,
        wide_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Dict:
        """
        Verify that the number of non-null values in all entity-specific
        columns of the wide table equals the row count of the long table.

        Each non-null cell in the wide table is expected to produce exactly
        one row in the long table after null rows are dropped.

        Only source_variable rows that carry a target_code or target_term are
        counted; affiliated/ancillary columns (empty target_code AND target_term)
        do not generate independent long-table rows and are excluded.
        """
        # Restrict to columns that will actually generate long-table rows
        mapped = entity_config[
            entity_config["target_code"].str.strip().ne("") |
            entity_config["target_term"].str.strip().ne("")
        ]
        source_columns = mapped["source_variable"].unique().tolist()

        # Only include columns that actually exist in the wide table
        present_cols = [c for c in source_columns if c in wide_df.columns]
        missing_from_wide = [c for c in source_columns if c not in wide_df.columns]

        # Count qualifying wide-table cells: non-null, and not in skipped_values
        if present_cols:
            sub = wide_df[present_cols]
            sv_set = self._build_skip_set()
            if sv_set is not None:
                mask = sub.notna() & ~sub.astype(str).isin(sv_set)
            else:
                mask = sub.notna()
            expected_rows = int(mask.sum().sum())
        else:
            expected_rows = 0

        actual_rows = len(long_df)
        match = expected_rows == actual_rows
        delta = actual_rows - expected_rows

        return {
            "source_columns_in_config": len(entity_config["source_variable"].unique()),
            "source_columns_counted": len(source_columns),
            "source_columns_found_in_wide": len(present_cols),
            "source_columns_missing_from_wide": missing_from_wide,
            "skipped_values": self.skipped_values,
            "expected_long_rows": expected_rows,
            "actual_long_rows": actual_rows,
            "delta (actual - expected)": delta,
            "PASS": match,
        }

    # ------------------------------------------------------------------
    # Check 2 — Column Coverage
    # ------------------------------------------------------------------

    def check_column_coverage(
        self,
        entity: str,
        prefix: str,
        entity_config: pd.DataFrame,
        wide_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Dict:
        """
        Ensure every source_label that has a configured target_code or
        target_term has produced at least one row in the long table's
        ``{prefix}_source_text`` column.

        Labels whose source_variable has no qualifying values in the wide
        table (all null, all zero, or absent) are separated as
        ``expected_empty`` — they do **not** cause a FAIL.  Only labels
        where the wide table contains qualifying data but the long table has
        no corresponding row are counted as true gaps and cause a FAIL.
        """
        source_text_col = f"{prefix}_source_text"

        if source_text_col not in long_df.columns:
            return {
                "source_text_column": source_text_col,
                "ERROR": f"Column '{source_text_col}' not found in long table.",
                "PASS": False,
            }

        # Only check source_labels that are expected to produce records
        mapped_config = entity_config[
            entity_config["target_code"].str.strip().ne("") |
            entity_config["target_term"].str.strip().ne("")
        ]
        expected_sources: set = set(mapped_config["source_label"].unique())
        observed_sources: set = set(long_df[source_text_col].dropna().unique())

        not_produced_raw = sorted(expected_sources - observed_sources)
        unexpected = sorted(observed_sources - expected_sources)

        # Build source_label → source_variable lookup for wide-table check
        label_to_var = (
            mapped_config[["source_label", "source_variable"]]
            .drop_duplicates()
            .set_index("source_label")["source_variable"]
            .to_dict()
        )

        # Classify not_produced labels:
        #   true_gaps     – wide column has at least one non-null, non-zero value
        #                   but the label never appears in the long table → genuine ETL gap
        #   expected_empty – wide column is absent, entirely null, or entirely
        #                   zero/falsy (e.g. binary checkbox where no participant
        #                   answered "yes") → no records expected, not a gap
        sv_set_c2 = self._build_skip_set()

        true_gaps: List[str] = []
        expected_empty: List[str] = []
        for label in not_produced_raw:
            var = label_to_var.get(label, "")
            if var and var in wide_df.columns:
                col = wide_df[var]
                non_null = col.dropna()
                if sv_set_c2 is not None:
                    has_qualifying = bool(
                        len(non_null) > 0
                        and (~non_null.astype(str).isin(sv_set_c2)).any()
                    )
                else:
                    has_qualifying = bool(
                        len(non_null) > 0
                        and ((non_null != 0) & (non_null != "") & (non_null != "0")).any()
                    )
                if has_qualifying:
                    true_gaps.append(label)
                else:
                    expected_empty.append(label)
            else:
                # Variable absent from wide table → expected empty
                expected_empty.append(label)

        return {
            "source_text_column": source_text_col,
            "expected_source_columns": len(expected_sources),
            "observed_source_columns": len(observed_sources),
            "true_gaps (has wide data but absent from long table)": true_gaps,
            "expected_empty (no qualifying values in wide table)": expected_empty,
            "unexpected (in long table but not in config)": unexpected,
            "PASS": len(true_gaps) == 0,
        }

    # ------------------------------------------------------------------
    # Check 3 — Participant Distribution
    # ------------------------------------------------------------------

    def check_participant_distribution(
        self,
        entity: str,
        prefix: str,
        entity_config: pd.DataFrame,
        wide_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Dict:
        """
        Build a prevalence table anchored to the mapping config: every
        (source_variable, target_code, target_term) tuple that has a configured
        target_code or target_term gets its own row, with ``unique_participants``
        set to 0 when no long-table records were produced for it.

        Rows are ordered by source_variable's position in the config (input
        order), then by target_code within the same source_variable.
        """
        code_col = f"{prefix}_code"
        source_text_col = f"{prefix}_source_text"
        wide_pid_col = self.wide_pid_col
        long_pid_col = self.long_pid_col

        total_participants = wide_df[wide_pid_col].nunique()

        if code_col not in long_df.columns:
            return {
                "code_column": code_col,
                "ERROR": f"Column '{code_col}' not found in long table.",
                "PASS": False,
            }

        if long_pid_col not in long_df.columns:
            return {
                "code_column": code_col,
                "ERROR": (
                    f"Participant ID column '{long_pid_col}' not found in long table."
                ),
                "PASS": False,
            }

        # All (source_variable, source_label, target_code, target_term) rows from
        # config that are expected to produce long-table records.
        mapped_config = (
            entity_config[
                entity_config["target_code"].str.strip().ne("") |
                entity_config["target_term"].str.strip().ne("")
            ][["source_variable", "source_label", "target_code", "target_term"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Ordered unique source variables from the config (preserves file input order)
        config_var_order = {
            v: i for i, v in enumerate(entity_config["source_variable"].unique())
        }

        # Map source_label → source_variable for annotating long-table rows
        label_to_var = (
            entity_config[["source_label", "source_variable"]]
            .drop_duplicates()
            .set_index("source_label")["source_variable"]
            .to_dict()
        )

        # Annotate long-table rows with their originating source_variable
        working = long_df.dropna(subset=[code_col]).copy()
        if source_text_col in working.columns:
            working["_source_var"] = working[source_text_col].map(label_to_var)
        else:
            working["_source_var"] = ""

        # Unique participant count per (source_variable, code) pair
        if not working.empty:
            actual_counts = (
                working.dropna(subset=["_source_var"])
                .groupby(["_source_var", code_col])[long_pid_col]
                .nunique()
                .rename("unique_participants")
                .reset_index()
                .rename(columns={"_source_var": "source_variable", code_col: "target_code"})
            )
        else:
            actual_counts = pd.DataFrame(
                columns=["source_variable", "target_code", "unique_participants"]
            )

        # Left-join config with actual counts so all configured rows appear,
        # defaulting to 0 participants when no long-table records were produced.
        merged = mapped_config.merge(
            actual_counts, on=["source_variable", "target_code"], how="left"
        )
        merged["unique_participants"] = merged["unique_participants"].fillna(0).astype(int)

        # Sort by config input order, then by target_code within each variable
        merged["_var_order"] = (
            merged["source_variable"]
            .map(config_var_order)
            .fillna(len(config_var_order))
        )
        merged = merged.sort_values(["_var_order", "target_code"]).drop(columns=["_var_order"])

        merged["pct_of_cohort"] = (
            merged["unique_participants"] / total_participants * 100
        ).round(2)

        total_configured = len(merged)
        # Rare: codes that did appear but with very few participants
        rare_count = int(
            merged["unique_participants"].between(1, self.rare_threshold).sum()
        )
        # Observed codes = those with at least one participant
        total_observed_codes = int((merged["unique_participants"] > 0).sum())

        all_records: List[Dict] = []
        for _, row in merged.iterrows():
            all_records.append({
                "source_variable": str(row["source_variable"] or ""),
                "source_label": str(row["source_label"] or ""),
                "target_code": str(row["target_code"] or ""),
                "target_term": str(row["target_term"] or ""),
                "unique_participants": int(row["unique_participants"]),
                "pct_of_cohort": float(row["pct_of_cohort"]),
            })

        return {
            "code_column": code_col,
            "total_cohort_participants (from wide table)": total_participants,
            "total_configured_entries": total_configured,
            "total_observed_entries": total_observed_codes,
            "all_codes_by_prevalence": all_records,
            f"rare_codes (1 <= unique_participants <= {self.rare_threshold})": rare_count,
            "rare_pct_of_observed": (
                round(rare_count / total_observed_codes * 100, 2)
                if total_observed_codes
                else 0.0
            ),
            "PASS": True,  # Informational check; always passes
        }

    # ------------------------------------------------------------------
    # Check 4 — Per-Variable Gap Analysis
    # ------------------------------------------------------------------

    def check_variable_gaps(
        self,
        entity: str,
        prefix: str,
        entity_config: pd.DataFrame,
        wide_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Dict:
        """
        For every source_variable that carries a target_code or target_term,
        compare the qualifying wide-table value count against the number of
        long-table rows whose {prefix}_source_text matches that variable's
        source_label.

        ``self.skipped_values`` controls which wide-table cell values are
        excluded from the qualifying count:
        - None (default) → all non-null cells qualify.
        - A list such as ["0", "-1"] → non-null cells whose string value is
          in the list are excluded (e.g. "not performed" / "unknown" sentinel).

        Rows are returned in the source_variable order from the config file.
        Rows with a non-zero delta reveal variables that were not converted
        (negative delta) or were duplicated (positive delta).
        """
        source_text_col = f"{prefix}_source_text"

        mapped = entity_config[
            entity_config["target_code"].str.strip().ne("") |
            entity_config["target_term"].str.strip().ne("")
        ][["source_variable", "source_label"]].drop_duplicates()

        has_source_text = source_text_col in long_df.columns
        long_counts: Dict[str, int] = {}
        if has_source_text:
            long_counts = (
                long_df[source_text_col]
                .dropna()
                .value_counts()
                .to_dict()
            )

        # Precompute normalised skip set (covers both int-string and float-string forms)
        sv_set = self._build_skip_set()

        rows: List[Dict] = []
        for config_order, (_, cfg_row) in enumerate(mapped.iterrows()):
            var = cfg_row["source_variable"]
            label = cfg_row["source_label"]

            if var in wide_df.columns:
                col = wide_df[var]
                non_null = col.dropna()
                if sv_set is not None:
                    # Count non-null values NOT in skipped_values
                    wide_count = int((~non_null.astype(str).isin(sv_set)).sum())
                else:
                    wide_count = int(len(non_null))
            else:
                wide_count = None

            long_count = int(long_counts.get(label, 0)) if has_source_text else None
            delta = (
                (long_count - wide_count)
                if (wide_count is not None and long_count is not None)
                else None
            )

            rows.append({
                "config_order": config_order,
                "source_variable": var,
                "source_label": label,
                "wide_count": wide_count,
                "long_count": long_count,
                "delta": delta,
                "missing_in_wide": wide_count is None,
            })

        mismatched = [r for r in rows if r["delta"] not in (0, None)]
        missing_in_wide = [r for r in rows if r["missing_in_wide"]]
        perfect = len(rows) - len(mismatched) - len(missing_in_wide)

        return {
            "source_text_column": source_text_col,
            "skipped_values": self.skipped_values,
            "total_variables_checked": len(rows),
            "perfect_match": perfect,
            "mismatched": mismatched,
            "missing_in_wide": missing_in_wide,
            "PASS": len(mismatched) == 0,
        }

    # ------------------------------------------------------------------
    # Report rendering helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _bool_badge(value: bool) -> str:
        return "✔ PASS" if value else "✖ FAIL"

    def _build_skip_set(self) -> Optional[set]:
        """
        Build a normalised string set for matching against wide-table cell
        values that should be skipped.

        Pandas reads numeric columns as float64, so a source value of ``0``
        becomes ``0.0`` in the DataFrame, and ``str(0.0)`` is ``"0.0"`` not
        ``"0"``.  This helper expands every skip value to cover both the
        plain integer-string form (``"0"``, ``"-1"``) and the float-string
        form (``"0.0"``, ``"-1.0"``) so comparisons work regardless of how
        pandas inferred the column dtype.

        Returns ``None`` when ``self.skipped_values`` is not set.
        """
        if self.skipped_values is None:
            return None
        result: set = set()
        for v in self.skipped_values:
            s = str(v)
            result.add(s)
            try:
                f = float(s)
                result.add(str(f))       # e.g. "0" → "0.0"
                result.add(str(int(f)))  # e.g. "0.0" → "0"
            except (ValueError, OverflowError):
                pass
        return result

    def _render_completeness(self, result: Dict) -> List[str]:
        sv = result.get("skipped_values")
        wide_basis = (
            f"non-null cells excluding {sv}" if sv is not None else "all non-null cells"
        )
        lines = [
            "  ── Check 1: Completeness ──────────────────────────────────────",
            f"  Source variables in config              : {result['source_columns_in_config']}",
            f"  Source variables with code/term (counted): {result['source_columns_counted']}",
            f"  Source variables found in wide table    : {result['source_columns_found_in_wide']}",
            f"  Wide count basis                        : {wide_basis}",
        ]
        if result["source_columns_missing_from_wide"]:
            lines.append(
                f"  ⚠ Columns in config but NOT in wide table:\n"
                + "\n".join(f"      - {c}" for c in result["source_columns_missing_from_wide"])
            )
        lines += [
            f"  Expected long-table rows           : {result['expected_long_rows']:,}",
            f"  Actual long-table rows             : {result['actual_long_rows']:,}",
            f"  Delta (actual − expected)          : {result['delta (actual - expected)']:+,}",
            f"  Result  →  {self._bool_badge(result['PASS'])}",
        ]
        return lines

    def _render_coverage(self, result: Dict) -> List[str]:
        lines = [
            "  ── Check 2: Column Coverage ────────────────────────────────────",
            f"  Source-text column   : {result.get('source_text_column', 'N/A')}",
        ]
        if "ERROR" in result:
            lines.append(f"  ✖ ERROR: {result['ERROR']}")
            return lines

        lines += [
            f"  Expected source cols (mapped only) : {result['expected_source_columns']}",
            f"  Observed source cols               : {result['observed_source_columns']}",
        ]

        true_gaps = result.get("true_gaps (has wide data but absent from long table)", [])
        expected_empty = result.get("expected_empty (no qualifying values in wide table)", [])
        unexpected = result.get("unexpected (in long table but not in config)", [])

        if true_gaps:
            lines.append(
                f"  ⚠ Not produced — wide data present but absent from long table ({len(true_gaps)}):"
            )
            lines += [f"      - {c}" for c in true_gaps]
        else:
            lines.append("  ✔ All expected source columns produced output.")

        if expected_empty:
            lines.append(
                f"  ℹ Expected empty — no qualifying values in wide table ({len(expected_empty)}):"
            )
            lines += [f"      - {c}" for c in expected_empty]

        if unexpected:
            lines.append("  ⚠ Unexpected source_text values (not in config):")
            lines += [f"      - {c}" for c in unexpected]

        lines.append(f"  Result  →  {self._bool_badge(result['PASS'])}")
        return lines

    def _render_distribution(self, result: Dict) -> List[str]:
        lines = [
            "  ── Check 3: Participant Distribution ───────────────────────────",
            f"  Code column                  : {result.get('code_column', 'N/A')}",
        ]
        if "ERROR" in result:
            lines.append(f"  ✖ ERROR: {result['ERROR']}")
            return lines

        total = result["total_cohort_participants (from wide table)"]
        rare_key = [k for k in result if k.startswith("rare_codes")][0]

        lines += [
            f"  Total cohort participants     : {total:,}",
            f"  Total configured entries      : {result['total_configured_entries']:,}",
            f"  Entries with participants > 0 : {result['total_observed_entries']:,}",
            f"  {rare_key}  : {result[rare_key]} "
            f"({result['rare_pct_of_observed']}% of observed entries)",
            "",
            f"  {'SOURCE_VARIABLE':<25} {'SOURCE_LABEL':<30} {'CODE':<18} {'TERM':<30} {'PARTICIPANTS':>12}  {'%COHORT':>8}",
            f"  {'-'*25} {'-'*30} {'-'*18} {'-'*30} {'-'*12}  {'-'*8}",
        ]
        for rec in result["all_codes_by_prevalence"]:
            var_str = str(rec["source_variable"])[:24]
            lbl_str = str(rec["source_label"])[:29]
            code_str = str(rec["target_code"])[:17]
            term_str = str(rec["target_term"])[:29] if rec["target_term"] else ""
            lines.append(
                f"  {var_str:<25} {lbl_str:<30} {code_str:<18} {term_str:<30} "
                f"{rec['unique_participants']:>12,}  {rec['pct_of_cohort']:>7.2f}%"
            )
        lines.append(f"  Result  →  {self._bool_badge(result['PASS'])}  (informational)")
        return lines

    def _render_variable_gaps(self, result: Dict) -> List[str]:
        sv = result.get("skipped_values")
        wide_basis = (
            f"non-null cells excluding {sv}" if sv is not None else "all non-null cells"
        )
        lines = [
            "  ── Check 4: Per-Variable Gap Analysis ──────────────────────────",
            f"  Source-text column       : {result.get('source_text_column', 'N/A')}",
            f"  Wide count basis         : {wide_basis}",
            f"  Variables checked        : {result['total_variables_checked']}",
            f"  Perfect match (delta=0)  : {result['perfect_match']}",
            f"  Mismatched               : {len(result['mismatched'])}",
        ]

        if result["missing_in_wide"]:
            lines.append("  ⚠ Variables in config but absent from wide table:")
            for r in result["missing_in_wide"]:
                lines.append(f"      - {r['source_variable']}  ({r['source_label']})")

        if result["mismatched"]:
            lines += [
                "",
                f"  {'SOURCE_VARIABLE':<35} {'WIDE':>6}  {'LONG':>6}  {'DELTA':>6}  NOTE",
                f"  {'-'*35} {'-'*6}  {'-'*6}  {'-'*6}  {'-'*22}",
            ]
            for r in sorted(result["mismatched"], key=lambda x: x["config_order"]):
                wide = r["wide_count"] if r["wide_count"] is not None else "N/A"
                long = r["long_count"] if r["long_count"] is not None else "N/A"
                delta = r["delta"] if r["delta"] is not None else "N/A"
                note = "under-converted" if isinstance(delta, int) and delta < 0 else "over-converted"
                var_str = str(r["source_variable"])[:34]
                lines.append(
                    f"  {var_str:<35} {wide:>6}  {long:>6}  {delta:>+6}  {note}"
                )
        else:
            lines.append("  ✔ All variables converted completely — no gaps detected.")

        lines.append(f"  Result  →  {self._bool_badge(result['PASS'])}")
        return lines

    # ------------------------------------------------------------------
    # Main orchestrator
    # ------------------------------------------------------------------

    def generate_report(
        self,
        wide_path: Path,
        long_dir: Path,
        config_path: Path,
        output_dir: Optional[Path] = None,
        entities: Optional[List[str]] = None,
    ) -> bool:
        """
        Run all checks for every entity in the config and emit a Data Health Report.

        Parameters
        ----------
        wide_path  : Path to the master wide CSV.
        long_dir   : Directory containing ``{entity}{long_suffix}`` files.
        config_path: Path to the mapping configuration CSV/TSV.
        output_dir : Optional directory to write per-entity output files.
                     Each entity produces two files in this directory:
                       ``{entity}_data_health_report.txt``  — full check report
                       ``{entity}_participant_distribution.tsv`` — Check 3 table
                     If None, only stdout output is produced.
        entities   : Optional list of entity names to validate.  When provided,
                     only those entities are checked; all others are skipped.

        Returns
        -------
        bool  True if every check for every entity passed, False otherwise.
        """
        config_df = self.load_config(config_path)
        wide_df = self.load_wide_table(wide_path)

        # --- Eligibility filter (participant.csv) ---
        roster = self.load_participant_roster(long_dir)
        if roster is not None:
            total_wide = len(wide_df)
            wide_df = wide_df[wide_df[self.wide_pid_col].astype(str).isin(roster.astype(str))]
            logger.info(
                "Eligibility filter applied: %d → %d participants retained.",
                total_wide,
                len(wide_df),
            )
            roster_note = (
                f"{len(wide_df):,} eligible (of {total_wide:,} in wide table) "
                f"— sourced from participant.csv"
            )
        else:
            roster_note = f"{wide_df[self.wide_pid_col].nunique():,} (no roster filter applied)"

        # --- Entity filter ---
        all_entities_in_config = sorted(config_df["entity"].unique())
        if entities:
            normalised_filter = [e.lower().strip() for e in entities]
            unknown = [e for e in normalised_filter if e not in all_entities_in_config]
            if unknown:
                logger.warning(
                    "Requested entity/entities not found in config and will be "
                    "skipped: %s.  Available: %s",
                    unknown,
                    all_entities_in_config,
                )
            target_entities = [e for e in all_entities_in_config if e in normalised_filter]
        else:
            target_entities = all_entities_in_config

        # Header lines are shared across the combined stdout report and each
        # per-entity file so the reader always has full provenance context.
        header_lines: List[str] = [
            SEPARATOR,
            "  DATA HEALTH REPORT",
            f"  Wide table    : {Path(wide_path).resolve()}",
            f"  Long-table dir: {Path(long_dir).resolve()}",
            f"  Config        : {Path(config_path).resolve()}",
            f"  Cohort        : {roster_note}",
            f"  Entities      : {', '.join(target_entities)}",
            SEPARATOR,
        ]

        report_lines: List[str] = list(header_lines)
        all_passed = True

        logger.info(
            "Validating %d entity/entities: %s", len(target_entities), target_entities
        )

        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

        for entity in target_entities:
            entity_config = config_df[config_df["entity"] == entity].copy()
            prefix = entity_config["prefix"].iloc[0]

            entity_section_lines: List[str] = [
                "",
                SEPARATOR,
                f"  ENTITY : {entity.upper()}   (column prefix: {prefix})",
                SEPARATOR,
            ]

            long_df = self.load_long_table(long_dir, entity)
            if long_df is None:
                entity_section_lines.append(
                    f"  \u2716 SKIPPED \u2014 long table file not found: "
                    f"{entity}{self.long_suffix}"
                )
                report_lines += entity_section_lines
                all_passed = False
                continue

            # Apply the same eligibility filter to the long table so that
            # all checks operate exclusively on eligible participants.
            if roster is not None and self.long_pid_col in long_df.columns:
                before = len(long_df)
                long_df = long_df[
                    long_df[self.long_pid_col].astype(str).isin(roster.astype(str))
                ]
                logger.info(
                    "Long table '%s': eligibility filter applied: %d → %d rows retained.",
                    entity,
                    before,
                    len(long_df),
                )

            # --- Check 1 ---
            c1 = self.check_completeness(entity, entity_config, wide_df, long_df)
            entity_section_lines += self._render_completeness(c1)
            if not c1["PASS"]:
                all_passed = False

            entity_section_lines.append("")

            # --- Check 2 ---
            c2 = self.check_column_coverage(entity, prefix, entity_config, wide_df, long_df)
            entity_section_lines += self._render_coverage(c2)
            if not c2["PASS"]:
                all_passed = False

            entity_section_lines.append("")

            # --- Check 3 ---
            c3 = self.check_participant_distribution(entity, prefix, entity_config, wide_df, long_df)
            entity_section_lines += self._render_distribution(c3)
            # Check 3 is informational; does not affect all_passed

            entity_section_lines.append("")

            # --- Check 4 ---
            c4 = self.check_variable_gaps(entity, prefix, entity_config, wide_df, long_df)
            entity_section_lines += self._render_variable_gaps(c4)
            if not c4["PASS"]:
                all_passed = False

            report_lines += entity_section_lines

            # --- Per-entity file output ---
            if output_dir:
                entity_passed = c1["PASS"] and c2["PASS"] and c4["PASS"]
                entity_result_lines: List[str] = [
                    "",
                    SEPARATOR,
                    f"  ENTITY RESULT  \u2192  "
                    f"{'✔ ALL CHECKS PASSED' if entity_passed else '✖ ONE OR MORE CHECKS FAILED'}",
                    SEPARATOR,
                ]
                entity_report_text = "\n".join(
                    header_lines + entity_section_lines + entity_result_lines
                )
                report_path = output_dir / f"{entity}_data_health_report.txt"
                report_path.write_text(entity_report_text, encoding="utf-8")
                logger.info("Entity report written to: %s", report_path)

                # Check 3 TSV
                records = c3.get("all_codes_by_prevalence", [])
                if records:
                    tsv_path = output_dir / f"{entity}_participant_distribution.tsv"
                    pd.DataFrame(records).to_csv(tsv_path, sep="\t", index=False)
                    logger.info("Participant distribution TSV written to: %s", tsv_path)

        report_lines += [
            "",
            SEPARATOR,
            f"  OVERALL RESULT  \u2192  "
            f"{'✔ ALL CHECKS PASSED' if all_passed else '✖ ONE OR MORE CHECKS FAILED'}",
            SEPARATOR,
        ]

        print("\n".join(report_lines))

        return all_passed


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate wide-to-long ETL output and produce a Data Health Report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--wide",
        required=True,
        metavar="PATH",
        help="Path to the master wide-format CSV file.",
    )
    parser.add_argument(
        "--long-dir",
        required=True,
        metavar="DIR",
        help="Directory containing the domain long-table CSV files.",
    )
    parser.add_argument(
        "--long-suffix",
        default=".csv",
        metavar="EXT",
        help=(
            "Filename suffix appended to the entity name to locate each "
            "long table (default: .csv, giving e.g. phenotype.csv)."
        ),
    )
    parser.add_argument(
        "--config",
        required=True,
        metavar="PATH",
        help="Path to the mapping_config.tsv file.",
    )
    parser.add_argument(
        "--entity",
        nargs="+",
        metavar="NAME",
        default=None,
        help=(
            "One or more entity names to validate "
            "(e.g. --entity phenotype medication). "
            "When omitted, all entities found in the config are validated."
        ),
    )
    parser.add_argument(
        "--wide-participant-id",
        default="participant_id",
        metavar="COL",
        help=(
            "Participant identifier column name in the wide table "
            "(default: participant_id)."
        ),
    )
    parser.add_argument(
        "--long-participant-id",
        default="participant_id",
        metavar="COL",
        help=(
            "Participant identifier column name in the long tables "
            "(default: participant_id)."
        ),
    )
    parser.add_argument(
        "--rare-threshold",
        type=int,
        default=1,
        metavar="N",
        help=(
            "Max unique participants for a code to be considered rare "
            "(default: 1)."
        ),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=5,
        metavar="N",
        help="Number of top codes to show in prevalence table (default: 5).",
    )
    parser.add_argument(
        "--skip-values",
        nargs="+",
        metavar="VAL",
        default=None,
        help=(
            "One or more wide-table cell values to exclude from expected-row counts "
            "in Checks 1, 2, and 4 (e.g. --skip-values 0 -1 for measurement entities "
            "where 0 means 'not performed' and -1 means 'unknown'). "
            "Values are matched as strings. When omitted, all non-null cells count."
        ),
    )
    parser.add_argument(
        "--output",
        metavar="DIR",
        default=None,
        help=(
            "Optional directory to write per-entity output files. "
            "Each entity produces {entity}_data_health_report.txt and "
            "{entity}_participant_distribution.tsv in this directory. "
            "When omitted, output is printed to stdout only."
        ),
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    validator = ETLValidator(
        wide_pid_col=args.wide_participant_id,
        long_pid_col=args.long_participant_id,
        rare_threshold=args.rare_threshold,
        top_n=args.top_n,
        long_suffix=args.long_suffix,
        skipped_values=args.skip_values,
    )

    try:
        passed = validator.generate_report(
            wide_path=Path(args.wide),
            long_dir=Path(args.long_dir),
            config_path=Path(args.config),
            output_dir=Path(args.output) if args.output else None,
            entities=args.entity,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        sys.exit(1)

    sys.exit(0 if passed else 2)


if __name__ == "__main__":
    main()
