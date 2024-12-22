import pandas as pd

def generate_mapping_table(df, operation_groups, operation_type, output_csv=None):
    """
    Generate a mapping table for all rows in the DataFrame and optionally save it to a CSV file.

    :param df: DataFrame containing the original data.
    :param operation_groups: List of groups of ProductIDs that participate in operations.
    :param operation_type: The type of operation performed (e.g., "merge", "aggregate").
    :param output_csv: Optional file path to save the mapping table as a CSV file.
    :return: Mapping table as a DataFrame.
    """
    mapping_table = []

    # Record operations for grouped rows
    for group in operation_groups:
        result_productid = f"P{len(df) + len(mapping_table) + 1}"  # Generate a new ProductID for the result
        for source_id in group:
            mapping_table.append({
                "source_id": source_id,
                "result_id": result_productid,
                "operation": operation_type
            })

    # Record "none" operation for rows not in groups
    all_source_ids = set(df["product_id"])
    grouped_source_ids = {source_id for group in operation_groups for source_id in group}
    ungrouped_source_ids = all_source_ids - grouped_source_ids

    for source_id in ungrouped_source_ids:
        mapping_table.append({
            "source_id": source_id,
            "result_id": source_id,
            "operation": "none"
        })

    # Convert to DataFrame
    mapping_df = pd.DataFrame(mapping_table)

    # Save to CSV if output_csv is provided
    if output_csv:
        mapping_df.to_csv(output_csv, index=False)

    return mapping_df


