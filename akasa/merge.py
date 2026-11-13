"""Three-way text merge.

This algorithm exists twice and must stay identical, statement for statement:
  Python      akasa/merge.py          three_way_merge_text   (the server merges on save)
  JavaScript  sutram/js/merge.js      threeWayMergeText      (the browser merges offline)
A parity test in the host application runs both on the same inputs. Change one side only with the other.

How it works: each side is diffed against the base into hunks (base_start, base_end, replacement lines), and the
two hunk lists are replayed over the base in order. Hunks that overlap are a conflict. Hunks that only touch are
merged when their order is unambiguous: two edits of neighbouring lines, or an insertion next to an edit; two
different insertions at the same point are a conflict.

The diff: trim the common prefix and suffix, anchor on lines that occur exactly once on both sides (kept in an
increasing run), and diff the gaps between anchors the same way. A gap with no such line is solved exactly
with a longest-common-subsequence table when it is small, and treated as one replaced block when it is not.
"""
from typing import Dict, List, Optional, Tuple

_MERGE_LCS_CELL_LIMIT = 250000


def _merge_lcs_hunks(base_lines: List[str], b_lo: int, b_hi: int, target_lines: List[str], t_lo: int, t_hi: int, hunks: List[Tuple[int, int, List[str]]]) -> None:
    """Exact diff of base_lines[b_lo:b_hi] against target_lines[t_lo:t_hi] by a longest-common-subsequence table."""
    b_len = b_hi - b_lo
    t_len = t_hi - t_lo
    if b_len * t_len > _MERGE_LCS_CELL_LIMIT:
        hunks.append((b_lo, b_hi, target_lines[t_lo:t_hi]))
        return
    table = [[0] * (t_len + 1) for _ in range(b_len + 1)]
    for i in range(b_len - 1, -1, -1):
        row = table[i]
        below = table[i + 1]
        base_line = base_lines[b_lo + i]
        for j in range(t_len - 1, -1, -1):
            if base_line == target_lines[t_lo + j]:
                row[j] = 1 + below[j + 1]
            elif below[j] >= row[j + 1]:
                row[j] = below[j]
            else:
                row[j] = row[j + 1]
    i = 0
    j = 0
    while i < b_len or j < t_len:
        if i < b_len and j < t_len and base_lines[b_lo + i] == target_lines[t_lo + j]:
            i += 1
            j += 1
            continue
        start_i = i
        start_j = j
        while i < b_len or j < t_len:
            if i < b_len and j < t_len and base_lines[b_lo + i] == target_lines[t_lo + j]:
                break
            if j == t_len or (i < b_len and table[i + 1][j] >= table[i][j + 1]):
                i += 1
            else:
                j += 1
        hunks.append((b_lo + start_i, b_lo + i, target_lines[t_lo + start_j:t_lo + j]))


def _merge_unique_anchors(base_lines: List[str], b_lo: int, b_hi: int, target_lines: List[str], t_lo: int, t_hi: int) -> List[Tuple[int, int]]:
    """Pairs (base index, target index) of lines that occur exactly once in each range, as the longest run increasing on both sides."""
    base_seen: Dict[str, int] = {}
    for i in range(b_lo, b_hi):
        line = base_lines[i]
        base_seen[line] = -1 if line in base_seen else i
    target_seen: Dict[str, int] = {}
    for j in range(t_lo, t_hi):
        line = target_lines[j]
        target_seen[line] = -1 if line in target_seen else j
    pairs: List[Tuple[int, int]] = []
    for i in range(b_lo, b_hi):
        line = base_lines[i]
        if base_seen[line] == i:
            j = target_seen.get(line, -1)
            if j >= 0:
                pairs.append((i, j))
    # Longest increasing run of target indexes (patience sorting); pairs are already in base order.
    pile_tops: List[int] = []
    previous: List[int] = []
    for index in range(len(pairs)):
        target_index = pairs[index][1]
        lo = 0
        hi = len(pile_tops)
        while lo < hi:
            mid = (lo + hi) // 2
            if pairs[pile_tops[mid]][1] < target_index:
                lo = mid + 1
            else:
                hi = mid
        previous.append(pile_tops[lo - 1] if lo > 0 else -1)
        if lo == len(pile_tops):
            pile_tops.append(index)
        else:
            pile_tops[lo] = index
    anchors: List[Tuple[int, int]] = []
    index = pile_tops[-1] if pile_tops else -1
    while index >= 0:
        anchors.append(pairs[index])
        index = previous[index]
    anchors.reverse()
    return anchors


def _merge_diff_hunks(base_lines: List[str], target_lines: List[str]) -> List[Tuple[int, int, List[str]]]:
    """Hunks (base_start, base_end, replacement_lines), in base order, that turn base_lines into target_lines."""
    hunks: List[Tuple[int, int, List[str]]] = []
    pending = [(0, len(base_lines), 0, len(target_lines))]
    while pending:
        b_lo, b_hi, t_lo, t_hi = pending.pop()
        while b_lo < b_hi and t_lo < t_hi and base_lines[b_lo] == target_lines[t_lo]:
            b_lo += 1
            t_lo += 1
        while b_hi > b_lo and t_hi > t_lo and base_lines[b_hi - 1] == target_lines[t_hi - 1]:
            b_hi -= 1
            t_hi -= 1
        if b_lo == b_hi and t_lo == t_hi:
            continue
        if b_lo == b_hi or t_lo == t_hi:
            hunks.append((b_lo, b_hi, target_lines[t_lo:t_hi]))
            continue
        anchors = _merge_unique_anchors(base_lines, b_lo, b_hi, target_lines, t_lo, t_hi)
        if not anchors:
            _merge_lcs_hunks(base_lines, b_lo, b_hi, target_lines, t_lo, t_hi, hunks)
            continue
        # Gaps are pushed last to first, so they are popped, and their hunks appended, in base order.
        gaps = []
        gap_b = b_lo
        gap_t = t_lo
        for anchor_b, anchor_t in anchors:
            gaps.append((gap_b, anchor_b, gap_t, anchor_t))
            gap_b = anchor_b + 1
            gap_t = anchor_t + 1
        gaps.append((gap_b, b_hi, gap_t, t_hi))
        for index in range(len(gaps) - 1, -1, -1):
            pending.append(gaps[index])
    return hunks


def _merge_contains_run(needle: List[str], haystack: List[str]) -> bool:
    """True when the lines of needle appear in haystack as one unbroken run."""
    if not needle or len(needle) > len(haystack):
        return False
    for start in range(len(haystack) - len(needle) + 1):
        if haystack[start:start + len(needle)] == needle:
            return True
    return False


def _merge_goes_first(first: Tuple[int, int, List[str]], second: Tuple[int, int, List[str]]) -> bool:
    """True when hunk `first` lies wholly before hunk `second`: a gap between them, or touching with an unambiguous order."""
    if first[1] < second[0]:
        return True
    return first[1] == second[0] and (first[0] < first[1] or second[0] < second[1])


def three_way_merge_text(base_text: str, disk_text: str, incoming_text: str) -> Tuple[bool, str]:
    """
    Deterministic three-way text merge of two edits of base_text: the one on disk and the incoming one.
    Returns (is_clean_merge, merged_text); on a conflict, (False, incoming_text).
    """
    if disk_text == incoming_text:
        return True, incoming_text
    if disk_text == base_text:
        return True, incoming_text
    if incoming_text == base_text:
        return True, disk_text

    base_norm = base_text.replace('\r\n', '\n')
    disk_norm = disk_text.replace('\r\n', '\n')
    inc_norm = incoming_text.replace('\r\n', '\n')

    if disk_norm == inc_norm:
        return True, incoming_text
    if disk_norm == base_norm:
        return True, incoming_text
    if inc_norm == base_norm:
        return True, disk_text

    base_lines = base_norm.split('\n')
    disk_hunks = _merge_diff_hunks(base_lines, disk_norm.split('\n'))
    inc_hunks = _merge_diff_hunks(base_lines, inc_norm.split('\n'))

    # 1. Late acknowledgement: disk holds an earlier save of the same edits (each of its hunks is, on the same base
    #    lines, equal to or an unbroken run inside an incoming hunk), so incoming already contains it.
    disk_inside_incoming = True
    for d_hunk in disk_hunks:
        found = False
        for i_hunk in inc_hunks:
            if i_hunk[0] == d_hunk[0] and i_hunk[1] == d_hunk[1] and (d_hunk[2] == i_hunk[2] or _merge_contains_run(d_hunk[2], i_hunk[2])):
                found = True
                break
        if not found:
            disk_inside_incoming = False
            break
    if disk_inside_incoming:
        return True, incoming_text

    # 2. Replay both hunk lists over the base in order.
    out_lines: List[str] = []
    base_cursor = 0
    idx_d = 0
    idx_i = 0
    while idx_d < len(disk_hunks) or idx_i < len(inc_hunks):
        d_hunk: Optional[Tuple[int, int, List[str]]] = disk_hunks[idx_d] if idx_d < len(disk_hunks) else None
        i_hunk: Optional[Tuple[int, int, List[str]]] = inc_hunks[idx_i] if idx_i < len(inc_hunks) else None
        if d_hunk is not None and i_hunk is not None and d_hunk[0] == i_hunk[0] and d_hunk[1] == i_hunk[1] and d_hunk[2] == i_hunk[2]:
            chosen = d_hunk
            idx_d += 1
            idx_i += 1
        elif i_hunk is None or (d_hunk is not None and _merge_goes_first(d_hunk, i_hunk)):
            chosen = d_hunk
            idx_d += 1
        elif d_hunk is None or _merge_goes_first(i_hunk, d_hunk):
            chosen = i_hunk
            idx_i += 1
        else:
            return False, incoming_text
        out_lines.extend(base_lines[base_cursor:chosen[0]])
        out_lines.extend(chosen[2])
        base_cursor = chosen[1]
    out_lines.extend(base_lines[base_cursor:])
    merged_text = '\n'.join(out_lines)
    if '\r\n' in disk_text and '\r\n' not in merged_text:
        merged_text = merged_text.replace('\n', '\r\n')
    return True, merged_text
