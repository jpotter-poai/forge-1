# Forge Draft Surface

Use this reference when you are authoring a pipeline from scratch or recovering from MCP payload-shape friction.

## First Stops

1. Call `describe_pipeline_spec` before building a large `apply_pipeline_spec` payload.
2. Call `describe_block_type` before setting params on any block you have not used recently.
3. Trust `usage_notes` first; only open block source when the metadata appears incomplete or wrong.

## Orienting on a Pipeline: `render_pipeline_mermaid`

Use this tool whenever you open an unfamiliar or large pipeline and need to understand its structure before editing.

**Collapsed mode** — one node per comment block, inter-group edges only. Start here.

```json
{ "mode": "collapsed" }
```

Returns a Mermaid `graph TD` where each comment block is rendered as `GroupName\n(N nodes)`. Edge endpoints are contracted to the group boundary, so `group_A --> group_B` means *some node in A feeds some node in B*. Use this to understand the high-level DAG before drilling in.

**Detailed mode** — full nested subgraph structure (default).

```json
{ "mode": "detailed" }
```

Returns the same graph with nested `subgraph` blocks mirroring comment-block containment. Edges that cross a subgraph boundary use the subgraph ID as the endpoint, not the internal node. Use this to verify wiring, spot ungrouped nodes, or understand nested group structure.

**Edge contraction note:** In both modes, an edge `A --> B` where A or B is a subgraph ID means the actual data connection is between a node *inside* A and a node *inside* B. Use `inspect_pipeline` group memberships to find the specific nodes involved.

## Manual Comment Blocks: `add_comment`

Creates a non-managed comment annotation block positioned around a set of elements.

**With `member_ids` (preferred):** pass node IDs and/or existing comment IDs; the tool computes the bounding box with standard padding automatically.

```json
{
  "title": "Outlier Handling",
  "description": "Clips and imputes before model input",
  "member_ids": ["clip_outliers", "impute_missing", "log_transform"]
}
```

**With raw coordinates (fallback):** use when no member elements exist yet or you need precise placement.

```json
{
  "title": "Note",
  "description": "Placeholder for future steps",
  "x": 80,
  "y": 600,
  "width": 400,
  "height": 150
}
```

**Managed vs manual:** `add_comment` always creates a *manual* comment (`managed: false`). It will not be repositioned when `prettify` runs. If you want a comment block that tracks a group's layout automatically, create a group and call `prettify` — it will auto-generate and maintain that group's managed comment block.

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
