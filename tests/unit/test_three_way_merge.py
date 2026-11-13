"""Checks the three-way text merge the server runs when a save arrives from a changed base (akasa/merge.py)."""
import random
import unittest

from akasa.merge import three_way_merge_text
from akasa.vfs import three_way_merge_text as vfs_three_way_merge_text


def _apply(base, edits):
    """Applies non-overlapping (start, end, replacement) edits to a list of lines, last edit first."""
    out = list(base)
    for start, end, replacement in sorted(edits, reverse=True):
        out[start:end] = replacement
    return out


class TestThreeWayMerge(unittest.TestCase):
    """three_way_merge_text(base, disk, incoming) merges two edits of one base, or reports a conflict and loses nothing."""

    def assertMerges(self, base, disk, incoming, expected):
        self.assertEqual(three_way_merge_text(base, disk, incoming), (True, expected))

    def assertConflicts(self, base, disk, incoming):
        self.assertEqual(three_way_merge_text(base, disk, incoming), (False, incoming), "A conflict returns the incoming text untouched")

    def test_vfs_still_exposes_the_merge(self):
        self.assertIs(vfs_three_way_merge_text, three_way_merge_text)

    def test_one_sided_and_identical_changes(self):
        self.assertMerges("a\nb\nc", "A\nb\nc", "a\nb\nc", "A\nb\nc")
        self.assertMerges("a\nb\nc", "a\nb\nc", "a\nb\nC", "a\nb\nC")
        self.assertMerges("a\nb\nc", "a\nB\nc", "a\nB\nc", "a\nB\nc")
        self.assertMerges("a\nb", "a\nb", "a\nb", "a\nb")

    def test_changes_to_different_lines_merge(self):
        self.assertMerges("a\nb\nc", "A\nb\nc", "a\nb\nC", "A\nb\nC")
        self.assertMerges("a\nb\nc\nd", "a\nB\nc\nd", "a\nb\nC\nd", "a\nB\nC\nd")
        self.assertMerges("a\nb\nc", "a\nB\nc", "a\nb\nn\nc", "a\nB\nn\nc")
        self.assertMerges("a\nb\n", "A\nb\n", "a\nb\nc\n", "A\nb\nc\n")

    def test_overlapping_changes_conflict(self):
        self.assertConflicts("a\nb\nc", "a\nX\nc", "a\nY\nc")
        self.assertConflicts("a\nb\nc", "a\nc", "a\nB\nc")
        self.assertConflicts("a\nb", "a\nx\nb", "a\ny\nb")
        self.assertConflicts("", "x", "y")
        self.assertConflicts("Header\ncat\nFooter\n", "Header\ncat!\nFooter\n", "Header\nscatter!\nFooter\n")

    def test_a_deletion_is_not_dropped_next_to_a_changed_line(self):
        """Issue #18: this used to return the disk text as a clean merge, losing incoming's deleted line."""
        self.assertMerges("d\nc\nf\nf\nb\nb", "d\nf\nf\nb*\nb", "d\nc\nf\nf\nb", "d\nf\nf\nb*")
        # With repeated lines, an edit dropped by the old merge is now either merged or a conflict, never lost.
        self.assertMerges("c\nc", "c", "c*\nc", "c*")
        self.assertConflicts("f\na\na", "f\na2\na", "c4\nf\na")

    def test_late_acknowledgement_keeps_the_newer_save(self):
        """Disk holds an earlier save of the same typing (its reply was lost); the newer incoming text contains it."""
        v0 = "# Essay\n\nFirst paragraph.\n"
        v1 = v0 + "\nSecond paragraph.\n"
        v2 = v1 + "\nThird paragraph.\n"
        self.assertMerges(v0, v1, v2, v2)
        self.assertMerges("a\nb", "a\nx\nb", "a\nx\ny\nb", "a\nx\ny\nb")

    def test_windows_line_endings(self):
        self.assertMerges("a\nb\nc", "a\r\nb\r\nc", "a\nb\nC", "a\nb\nC")
        self.assertMerges("a\r\nb\r\nc\r\nd", "A\r\nb\r\nc\r\nd", "a\r\nb\r\nc\r\nD", "A\r\nb\r\nc\r\nD")

    def test_separated_edits_always_merge_to_both_edits(self):
        """On a base of distinct lines, edits kept at least one untouched line apart merge cleanly, in either order, to exactly both edits."""
        rng = random.Random(18)
        checked = 0
        for _ in range(1500):
            base = [f"line {i}" for i in range(rng.randint(4, 60))]
            edits = {"disk": [], "incoming": []}
            side = rng.choice(["disk", "incoming"])
            position = 0
            while position < len(base):
                length = rng.randint(0, 3)
                if position + length > len(base):
                    break
                if rng.random() < 0.5:
                    replacement = [f"new {rng.randint(0, 10 ** 6)}" for _ in range(rng.randint(0 if length else 1, 3))]
                    edits[side].append((position, position + length, replacement))
                    side = "incoming" if side == "disk" else "disk"
                position += length + rng.randint(1, 4)
            if not edits["disk"] or not edits["incoming"]:
                continue
            checked += 1
            base_text = "\n".join(base)
            disk_text = "\n".join(_apply(base, edits["disk"]))
            incoming_text = "\n".join(_apply(base, edits["incoming"]))
            expected = "\n".join(_apply(base, edits["disk"] + edits["incoming"]))
            self.assertEqual(three_way_merge_text(base_text, disk_text, incoming_text), (True, expected))
            self.assertEqual(three_way_merge_text(base_text, incoming_text, disk_text), (True, expected))
        self.assertGreater(checked, 500)

    def test_identities_hold_on_random_edits_with_repeated_lines(self):
        """Whatever the inputs: the same change twice is that change, a one-sided change wins, and a conflict returns incoming."""
        rng = random.Random(81)

        def mutate(lines):
            out = list(lines)
            for _ in range(rng.randint(1, 4)):
                index = rng.randint(0, len(out))
                operation = rng.choice(["insert", "delete", "change", "duplicate"])
                if operation == "insert":
                    out.insert(index, rng.choice(["", "}", "pass", f"new {rng.randint(0, 99)}"]))
                elif out and index < len(out):
                    if operation == "delete":
                        del out[index]
                    elif operation == "change":
                        out[index] = out[index] + "*"
                    else:
                        out.insert(index, out[index])
            return out

        for _ in range(1500):
            base = [rng.choice(["", "}", "pass", f"line {rng.randint(0, 30)}"]) for _ in range(rng.randint(0, 25))]
            base_text, a_text, b_text = "\n".join(base), "\n".join(mutate(base)), "\n".join(mutate(base))
            self.assertEqual(three_way_merge_text(base_text, a_text, a_text), (True, a_text))
            self.assertEqual(three_way_merge_text(base_text, base_text, b_text), (True, b_text))
            self.assertEqual(three_way_merge_text(base_text, a_text, base_text), (True, a_text))
            clean, merged = three_way_merge_text(base_text, a_text, b_text)
            if not clean:
                self.assertEqual(merged, b_text)

    def test_edits_far_apart_in_a_long_file_merge(self):
        """A long file with one edit near each end merges; the diff must not fall back to one whole-file block."""
        base = [f"row {i}" for i in range(20000)]
        disk = list(base)
        incoming = list(base)
        disk[10] = "changed near the top"
        incoming[19990] = "changed near the end"
        expected = list(disk)
        expected[19990] = "changed near the end"
        self.assertMerges("\n".join(base), "\n".join(disk), "\n".join(incoming), "\n".join(expected))


if __name__ == "__main__":
    unittest.main()
