from __future__ import annotations

import re
from typing import Any


def _safe_id(raw: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", raw)


def _esc(text: str) -> str:
    return text.replace('"', "&quot;").replace("\n", " ")


def _build_group_hierarchy(
    pipeline: dict[str, Any],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, str | None],
    dict[str, list[str]],
    dict[str, list[str]],
]:
    """
    Returns:
        groups          group_id -> group dict
        parent_of       group_id -> parent group_id (None = root), inferred from comment containment
        children_of     group_id -> [child group_ids]
        nodes_in_group  group_id -> [node_ids whose *primary* group is this group]
    """
    groups = {str(g["id"]): g for g in pipeline.get("groups", [])}
    nodes = {str(n["id"]): n for n in pipeline.get("nodes", [])}
    comments = {str(c["id"]): c for c in pipeline.get("comments", [])}

    def _group_rect(gid: str) -> tuple[float, float, float, float] | None:
        cid = str(groups[gid].get("comment_id") or "")
        c = comments.get(cid)
        if not c:
            return None
        pos = c.get("position") or {}
        return (
            float(pos.get("x", 0)),
            float(pos.get("y", 0)),
            float(c.get("width", 300)),
            float(c.get("height", 150)),
        )

    def _contains(
        outer: tuple[float, float, float, float],
        inner: tuple[float, float, float, float],
    ) -> bool:
        ox, oy, ow, oh = outer
        ix, iy, iw, ih = inner
        return ox <= ix and oy <= iy and (ox + ow) >= (ix + iw) and (oy + oh) >= (iy + ih)

    parent_of: dict[str, str | None] = {}
    for gid in groups:
        inner = _group_rect(gid)
        if inner is None:
            parent_of[gid] = None
            continue
        best_parent: str | None = None
        best_area = float("inf")
        for other_gid in groups:
            if other_gid == gid:
                continue
            outer = _group_rect(other_gid)
            if outer is None:
                continue
            if _contains(outer, inner):
                area = outer[2] * outer[3]
                if area < best_area:
                    best_area = area
                    best_parent = other_gid
        parent_of[gid] = best_parent

    children_of: dict[str, list[str]] = {gid: [] for gid in groups}
    for gid, parent in parent_of.items():
        if parent:
            children_of[parent].append(gid)

    nodes_in_group: dict[str, list[str]] = {gid: [] for gid in groups}
    for nid, node in nodes.items():
        gids = [str(g) for g in node.get("group_ids", [])]
        if gids:
            nodes_in_group.setdefault(gids[0], []).append(nid)

    return groups, parent_of, children_of, nodes_in_group


def _group_ancestor_path(gid: str, parent_of: dict[str, str | None]) -> list[str]:
    """[gid, parent, ..., root_group] (gid-to-root order)"""
    path = [gid]
    current = gid
    while parent_of.get(current):
        current = parent_of[current]
        path.append(current)
    return path


def _node_group_path(
    node_id: str,
    nodes: dict[str, dict[str, Any]],
    parent_of: dict[str, str | None],
) -> list[str]:
    """Path from node's primary group to root. Empty list when ungrouped."""
    gids = [str(g) for g in nodes.get(node_id, {}).get("group_ids", [])]
    if not gids:
        return []
    return _group_ancestor_path(gids[0], parent_of)


def _contracted_reps(
    src: str,
    tgt: str,
    nodes: dict[str, dict[str, Any]],
    parent_of: dict[str, str | None],
) -> tuple[str, str]:
    """
    Find the representative for src and tgt at the lowest common ancestor level.

    Each edge is contracted so it appears to leave/enter at the subgraph boundary
    rather than the internal node, giving the "same nesting level" semantics the
    user requested.  Returns (src_rep, tgt_rep) where each is either a group_id
    (subgraph boundary) or a node_id (for ungrouped or same-context nodes).
    """
    src_path = _node_group_path(src, nodes, parent_of)
    tgt_path = _node_group_path(tgt, nodes, parent_of)

    # Reverse so index 0 = root, last = immediate group
    src_rev = list(reversed(src_path))
    tgt_rev = list(reversed(tgt_path))

    # Common prefix length = depth of LCA in the group tree
    common = 0
    for s, t in zip(src_rev, tgt_rev):
        if s == t:
            common += 1
        else:
            break

    # The representative is the first group *below* the LCA, or the node itself
    # when the node sits directly at the LCA level (or is ungrouped).
    src_rep = src_rev[common] if common < len(src_rev) else src
    tgt_rep = tgt_rev[common] if common < len(tgt_rev) else tgt

    if not src_path:
        src_rep = src
    if not tgt_path:
        tgt_rep = tgt

    return src_rep, tgt_rep


def render_mermaid(
    pipeline: dict[str, Any],
    mode: str = "detailed",
    block_names: dict[str, str] | None = None,
) -> str:
    """
    Render a Mermaid ``graph TD`` diagram for the pipeline.

    mode='detailed'
        Full nested-subgraph rendering.  Comment blocks become ``subgraph``
        blocks, nested according to geometric containment.  Edges crossing
        subgraph boundaries are contracted to the subgraph boundary so they
        appear to leave/enter the group rather than the internal node.

    mode='collapsed'
        High-level map only.  Each comment block is rendered as a single node
        with a node-count label; internals are hidden.  Useful as a first pass
        before drilling into a specific group with inspect_pipeline / inspect_block.
    """
    nodes = {str(n["id"]): n for n in pipeline.get("nodes", [])}
    edges = pipeline.get("edges", [])
    groups, parent_of, children_of, nodes_in_group = _build_group_hierarchy(pipeline)
    bnames = block_names or {}

    def _node_label(nid: str) -> str:
        label = _esc(bnames.get(nid) or nodes.get(nid, {}).get("block", nid))
        return f'{_safe_id(nid)}["{label}\\n[{_esc(nid)}]"]'

    lines: list[str] = ["graph TD"]

    def _emit_contracted_edges() -> None:
        seen: set[tuple[str, str]] = set()
        for edge in edges:
            src = str(edge["source"])
            tgt = str(edge["target"])
            if src not in nodes or tgt not in nodes:
                continue
            src_rep, tgt_rep = _contracted_reps(src, tgt, nodes, parent_of)
            if src_rep == tgt_rep:
                continue
            key = (_safe_id(src_rep), _safe_id(tgt_rep))
            if key not in seen:
                seen.add(key)
                lines.append(f"    {_safe_id(src_rep)} --> {_safe_id(tgt_rep)}")

    if mode == "collapsed":

        def _all_node_count(gid: str) -> int:
            total = len(nodes_in_group.get(gid, []))
            for child in children_of.get(gid, []):
                total += _all_node_count(child)
            return total

        for gid, group in groups.items():
            name = group.get("name", gid)
            n = _all_node_count(gid)
            lines.append(f'    {_safe_id(gid)}["{_esc(name)}\\n({n} nodes)"]')

        for nid, node in nodes.items():
            if not node.get("group_ids"):
                lines.append(f"    {_node_label(nid)}")

        _emit_contracted_edges()

    else:  # detailed
        rendered: set[str] = set()

        def _render_subgraph(gid: str, indent: str) -> None:
            name = groups[gid].get("name", gid)
            lines.append(f'{indent}subgraph {_safe_id(gid)} ["{_esc(name)}"]')
            for nid in nodes_in_group.get(gid, []):
                lines.append(f"{indent}    {_node_label(nid)}")
                rendered.add(nid)
            for child in children_of.get(gid, []):
                _render_subgraph(child, indent + "    ")
            lines.append(f"{indent}end")

        root_groups = [gid for gid, p in parent_of.items() if p is None]
        for gid in root_groups:
            _render_subgraph(gid, "    ")

        for nid in nodes:
            if nid not in rendered:
                lines.append(f"    {_node_label(nid)}")

        _emit_contracted_edges()

    return "\n".join(lines)
