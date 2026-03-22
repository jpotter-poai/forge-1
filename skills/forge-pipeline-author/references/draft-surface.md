# Forge Draft Surface

Use this reference when you are authoring a pipeline from scratch or recovering from MCP payload-shape friction.

## First Stops

1. Call `describe_pipeline_spec` before building a large `apply_pipeline_spec` payload.
2. Call `describe_block_type` before setting params on any block you have not used recently.
3. Trust `usage_notes` first; only open block source when the metadata appears incomplete or wrong.

## `add_block`

- `params` may be:
  - a JSON object
  - a JSON string that decodes to an object
- `group_ids` may be:
  - a list of group IDs
  - a JSON string list
  - a comma-delimited string

Example:

```json
{
  "block_key": "LoadCSV",
  "node_id": "load_sales",
  "params": {"filepath": "C:\\Users\\you\\sales.csv"},
  "group_ids": ["group_load"]
}
```

## `apply_pipeline_spec`

Top-level keys:

- `name`
- `groups`
- `nodes`
- `edges`

Group spec:

```json
{
  "id": "group_load",
  "name": "Loading",
  "description": "Input roots",
  "member_node_ids": ["load_sales"]
}
```

Node spec:

```json
{
  "id": "scatter",
  "block": "MatrixScatterPlot",
  "params": {
    "x_column": "revenue",
    "y_column": "profit",
    "color_column": "segment",
    "color_mode": "categorical"
  },
  "group_ids": ["group_viz"]
}
```

Edge spec:

```json
{
  "id": "edge_load_scatter",
  "source": "load_sales",
  "target": "scatter"
}
```

Multi-input edge example:

```json
{
  "source": "assignments",
  "target": "group_means",
  "target_input": 1
}
```

## Behavior Notes Worth Watching

- `GroupMeanByAssignments` aligns its two inputs by row index before grouping.
- `MergeDatasets` treats input 0 as left and input 1 as right.
- `UMAPEmbed` appends embedding columns to the incoming frame.
- `KMeansClustering` appends the output cluster column to the incoming frame.
- `MatrixScatterPlot` uses a continuous color scale for numeric dtypes in `auto` mode and a discrete legend for string/object dtypes. Set `color_mode="categorical"` when numeric cluster labels should behave like categories.
