package tman;/*
 * TShapeIndex.java
 * =================
 *
 * Java port of the Python reference implementation (Tshape.py) of the
 * TShape spatial index described in the TMan paper:
 *
 *     TMan: A High-Performance Trajectory Data Management System
 *     Based on Key-value Stores, ICDE 2024.
 *
 * This is a line-for-line-in-spirit translation, not a from-scratch
 * redesign: every method here corresponds to one in Tshape.py, and the
 * long-form comments explaining *why* a piece of logic looks the way it
 * does (which parts of the paper are precisely implemented, which parts are
 * genuinely underspecified in the paper and required a documented choice,
 * and the two real bugs that were found and fixed during the Python
 * development of this index) have been carried over so this file is
 * readable on its own.
 *
 * No package declaration and no build system (Maven/Gradle) are used, by
 * request -- this is meant to be dropped into an existing Java project or
 * compiled directly:
 *
 *     javac TShapeIndex.java GeolifeExperiment.java
 *     java TShapeIndex                 // runs the small built-in smoke test
 *     java GeolifeExperiment --help    // runs the GeoLife benchmark port
 *
 * Coordinate convention
 * ----------------------
 * Trajectories are lists of Point(x, y). For GPS data use (longitude,
 * latitude), matching the Python version's convention.
 *
 * Dependencies
 * ------------
 * Only the standard library (java.util, java.security, java.math,
 * java.awt/javax.imageio for the optional PNG plot). No third-party jars.
 *
 * How a query flows through this file (read in this order)
 * ----------------------------------------------------------
 * 1. build() ingests a Map<String, List<Point>>. For every trajectory:
 *      computeMbr             -> bounding box in normalized [0,1]^2 space
 *      chooseResolution       -> Lemma 3/4: pick the finest quadtree depth r
 *                                whose alpha x beta window can hold the MBR
 *      findEnlargedElement    -> the (cx, cy, r) lower-left anchor cell
 *      computeShapeBitmap     -> alpha*beta-bit occupancy bitmap of the
 *                                trajectory inside that window ("raw shape")
 *    Then, once every trajectory has an (element, rawShape) pair:
 *      buildShapeDictionaries  -> per element, renumber the *observed* raw
 *                                 shapes (0..M-1) so spatially-similar shapes
 *                                 get adjacent ids (multiStartGreedyOrder)
 *      buildActivePrefixes     -> precompute which quadtree cells lead to
 *                                 real data, for fast query pruning later
 *      encodeQuadrantCode      -> Eq. 2: turn (cx, cy, r) into a single
 *                                 integer that preserves depth-first locality
 *    Finally each trajectory gets one integer key:
 *      tshapeValue = (quadrantCode << (alpha*beta)) | optimizedShapeId
 *    stored in invertedIndex (tshapeValue -> [trajectoryId, ...]) and
 *    primaryTable (toy rowkey -> raw trajectory), mirroring the paper's
 *    primary-table / secondary-index split.
 *
 * 2. queryCandidates(queryRect) walks the same quadtree top-down (Algorithm
 *    2 in the paper): prune branches that don't overlap the query, fast-path
 *    branches the query fully covers, and otherwise test each element's
 *    observed shapes individually. Returns a super-set of matching ids (a
 *    *filter*, by design -- see exactRangeQuery()).
 *
 * 3. exactRangeQuery(queryRect) = queryCandidates() + a per-segment
 *    geometric check against the real trajectory, to drop any false
 *    positives the coarse shape filter let through.
 *
 * Known simplifications versus the paper (see the relevant method's comment
 * for the full reasoning in each case):
 *   - Only the greedy shape-order heuristic is implemented, not the paper's
 *     genetic-algorithm alternative (multiStartGreedyOrder).
 *   - queryCandidates() does not use the paper's contiguous quadrant-code
 *     range shortcut for "SR fully covers Ei" -> "every descendant of Ei is
 *     a candidate too", because this prototype's enlarged elements slide to
 *     each trajectory's own MBR rather than sitting on a fixed
 *     non-overlapping grid (see the comment on queryCandidates() for the
 *     full argument).
 *   - TR / IDT / ST indexes (temporal and combined indexes) are out of
 *     scope; this file only reproduces the spatial TShape index.
 *
 * A note on 64-bit encoding (this is actually *more* faithful than Python)
 * --------------------------------------------------------------------------
 * The paper states its integer encoding needs 2*g + 1 + alpha*beta <= 64
 * bits to fit in a single long (see the discussion after Eq. 3). Python's
 * arbitrary-precision integers hide this constraint -- Tshape.py would
 * silently keep working (just slower, with bigger-than-64-bit Python ints)
 * even if you picked parameters that violate it. Java's `long` is a fixed
 * 64-bit type, so this port makes the paper's real constraint an explicit,
 * checked precondition in the constructor instead of a silent risk.
 */

import gr.ds.unipi.spatialnodb.messages.common.SpatialPoint;
import org.apache.commons.lang.RandomStringUtils;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import javax.imageio.ImageIO;

// =============================================================================
// Small value types (kept in this file, alongside TShapeIndex, so the whole
// index is still just "one file to read" -- see GeolifeExperiment.java for
// the only other file in this port).
// =============================================================================

/** A 2D point. For GPS data: x = longitude, y = latitude.
 *  Implements Serializable (not needed by the pure in-memory index, but
 *  required once Point[] values cross a Spark closure/broadcast boundary --
 *  see TShapeFrechetQuery.java). */
final class Point implements Serializable {
    public final double x;
    public final double y;

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return "(" + x + ", " + y + ")";
    }
}

/** An axis-aligned rectangle: (xmin, ymin, xmax, ymax). Used for MBRs, query
 *  rectangles, and enlarged-element/cell footprints alike. Serializable for
 *  the same reason as Point (see above). */
final class Bounds implements Serializable {
    public final double xmin;
    public final double ymin;
    public final double xmax;
    public final double ymax;

    public Bounds(double xmin, double ymin, double xmax, double ymax) {
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
    }

    @Override
    public String toString() {
        return "Bounds[" + xmin + ", " + ymin + ", " + xmax + ", " + ymax + "]";
    }
}

/** An enlarged element: the lower-left quadtree cell (cx, cy) at resolution
 *  r that anchors an alpha x beta window. Immutable value type used as a
 *  HashMap/HashSet key throughout TShapeIndex, so equals()/hashCode() matter. */
final class Element {
    public final int cx;
    public final int cy;
    public final int r;

    public Element(int cx, int cy, int r) {
        this.cx = cx;
        this.cy = cy;
        this.r = r;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Element)) {
            return false;
        }
        Element other = (Element) o;
        return cx == other.cx && cy == other.cy && r == other.r;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cx, cy, r);
    }

    @Override
    public String toString() {
        return "(" + cx + ", " + cy + ", " + r + ")";
    }
}

/**
 * Per-trajectory metadata produced by the TShape index (mirrors the Python
 * TShapeRecord dataclass).
 *
 * trajectoryId:      user-provided trajectory identifier.
 * element:           the selected enlarged element (cx, cy, r).
 * rawShape:          the alpha x beta occupancy bitmap before shape-code
 *                    optimization, as a long (bit i set means local cell i
 *                    -- see TShapeIndex.computeShapeBitmap -- is occupied).
 * optimizedShapeId:  the final per-element shape id after greedy Jaccard
 *                    ordering.
 * quadrantCode:      integer code of the lower-left quadtree cell of the
 *                    enlarged element (Eq. 2).
 * tshapeValue:       final integer key: (quadrantCode << (alpha*beta)) |
 *                    optimizedShapeId.
 * rowkey:            toy HBase-style rowkey used by this prototype.
 */
final class TShapeRecord {
    public final String trajectoryId;
    public final Element element;
    public final long rawShape;
    public final int optimizedShapeId;
    public final long quadrantCode;
    public final long tshapeValue;
    public final String rowkey;

    public TShapeRecord(
            String trajectoryId,
            Element element,
            long rawShape,
            int optimizedShapeId,
            long quadrantCode,
            long tshapeValue,
            String rowkey) {
        this.trajectoryId = trajectoryId;
        this.element = element;
        this.rawShape = rawShape;
        this.optimizedShapeId = optimizedShapeId;
        this.quadrantCode = quadrantCode;
        this.tshapeValue = tshapeValue;
        this.rowkey = rowkey;
    }
}

/**
 * One row of timing/selectivity data returned by
 * TShapeIndex.benchmarkSpatialQueries() -- mirrors the dict returned by the
 * Python method of the same name.
 */
final class QueryTiming {
    public final int numCandidates;
    public final int numExact;
    public final double candidateTimeS;
    public final double exactTimeS;
    public final double totalTimeS;

    public QueryTiming(int numCandidates, int numExact, double candidateTimeS, double exactTimeS, double totalTimeS) {
        this.numCandidates = numCandidates;
        this.numExact = numExact;
        this.candidateTimeS = candidateTimeS;
        this.exactTimeS = exactTimeS;
        this.totalTimeS = totalTimeS;
    }
}

/**
 * One result row of a discrete-Frechet similarity query
 * (TShapeIndex.frechetRangeQuery()).
 *
 * Carries the *actual points* of the matched trajectory, not just its id --
 * this is deliberate: an index lookup that only returns an id is not useful
 * to a caller unless they also have some other way to fetch the geometry, and
 * for the Parquet-backed version of this query (TShapeFrechetQuery.java)
 * there usually isn't one -- the Parquet row *is* the only copy of the
 * points. Keeping the same result shape here in the pure in-memory index
 * means both query paths are exercised against the same contract.
 */
final class FrechetMatch implements Serializable {
    public final String trajectoryId;
    public final List<Point> points;
    public final double distance;

    public FrechetMatch(String trajectoryId, List<Point> points, double distance) {
        this.trajectoryId = trajectoryId;
        this.points = points;
        this.distance = distance;
    }

    @Override
    public String toString() {
        return "FrechetMatch[" + trajectoryId + ", distance=" + distance + ", points=" + points.size() + "]";
    }
}

// =============================================================================
// Main index
// =============================================================================

/**
 * TShape spatial index prototype (Java port of the Python TShapeIndex class).
 *
 * See the module-level comment at the top of this file for a step-by-step
 * walkthrough of how build() and the query methods fit together.
 */
public class TShapeIndex {

    // -------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------

    public final int alpha;
    public final int beta;
    public final int g; // max_resolution in the Python version
    public Bounds bounds; // nullable; computed from the dataset if not given
    public final int numShards;
    public final int greedyStarts;
    public final long greedyWorkBudget;
    public final long randomSeed;

    // -------------------------------------------------------------------
    // Index state, populated by build()
    // -------------------------------------------------------------------

    /** In a real key-value system this would be a primary table keyed by
     *  shard :: TShapeValue :: trajectoryId. Rowkey -> raw trajectory. */
    public final Map<String, List<Point>> primaryTable = new LinkedHashMap<>();

    /** tshapeValue -> trajectory ids. In-memory stand-in for key-value scans
     *  over rowkey ranges. */
    public final Map<Long, List<String>> invertedIndex = new LinkedHashMap<>();

    /** trajectoryId -> metadata, useful for debugging and evaluation. */
    public final Map<String, TShapeRecord> records = new LinkedHashMap<>();

    /** element -> {rawShape -> optimizedShapeId}. Logically equivalent to
     *  the Redis/cache mapping described in the paper. */
    public final Map<Element, Map<Long, Integer>> shapeDictionary = new LinkedHashMap<>();

    /** element -> {optimizedShapeId -> rawShape}, the inverse of
     *  shapeDictionary, used when scanning an element's observed shapes. */
    public final Map<Element, Map<Integer, Long>> reverseShapeDictionary = new LinkedHashMap<>();

    /** Every (cx, cy, r) that is either a real indexed enlarged element, or
     *  an ancestor cell of one. Built once per build() call by
     *  buildActivePrefixes() and lets queryCandidates() do a breadth-first
     *  quadtree walk that never wastes time descending into empty regions
     *  of the domain. See queryCandidates() for how it is used. */
    private Set<Element> activePrefixes = new HashSet<>();

    /** Deepest resolution actually used by any indexed element. Used to know
     *  when to stop the BFS in queryCandidates() from recursing further. */
    private int maxIndexedResolution = 0;

    // -------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------

    /** Convenience constructor using the same defaults as the Python
     *  version: alpha=3, beta=3, maxResolution=16, no fixed bounds,
     *  numShards=16, greedyStarts=32, greedyWorkBudget=2,000,000,
     *  randomSeed=42. */
    public TShapeIndex() {
        this(3, 3, 16, null, 16, 32, 2_000_000L, 42L);
    }

    /**
     * @param alpha, beta       Size of the local enlarged element, measured
     *                          in quadtree cells. The paper evaluates values
     *                          such as 3x3 and 5x5.
     * @param maxResolution     Maximum quadtree depth. Larger values allow
     *                          finer spatial cells.
     * @param bounds            Optional fixed global spatial bounds. If
     *                          null, bounds are computed from the input
     *                          dataset when build() is called.
     * @param numShards         Number of toy rowkey shards, mimicking the
     *                          hash prefix used by key-value stores to
     *                          reduce hotspotting.
     * @param greedyStarts      Upper bound on the number of random starting
     *                          shapes used by the multistart greedy
     *                          shape-code optimizer (see
     *                          multiStartGreedyOrder). The actual number of
     *                          starts used for a given enlarged element is
     *                          also capped by greedyWorkBudget.
     * @param greedyWorkBudget  Soft ceiling on the amount of work (roughly
     *                          starts * numShapes^2 pairwise comparisons)
     *                          spent optimizing the shape order of any
     *                          single enlarged element. See
     *                          multiStartGreedyOrder for the full reasoning
     *                          (real datasets can have elements with
     *                          thousands of observed shapes).
     * @param randomSeed        Seed for reproducible shape ordering. Note:
     *                          Java's java.util.Random uses a different
     *                          algorithm than Python's random module, so the
     *                          exact same seed will *not* reproduce the same
     *                          shuffle order as the Python version -- each
     *                          language's output is only reproducible
     *                          against itself.
     */
    public TShapeIndex(
            int alpha,
            int beta,
            int maxResolution,
            Bounds bounds,
            int numShards,
            int greedyStarts,
            long greedyWorkBudget,
            long randomSeed) {
        if (alpha < 2 || beta < 2) {
            throw new IllegalArgumentException("alpha and beta must be >= 2");
        }
        if (maxResolution < 0) {
            throw new IllegalArgumentException("maxResolution must be non-negative");
        }
        // The paper's own encoding requires 2*g + 1 + alpha*beta <= 64 bits
        // to fit the quadrant code and shape id together in one 64-bit
        // value (see the discussion after Eq. 3). Python hides this
        // constraint behind arbitrary-precision integers; Java's `long` is
        // a real 64-bit type, so we check it explicitly here rather than
        // silently overflowing during encodeQuadrantCode()/build().
        long requiredBits = 2L * maxResolution + 1 + (long) alpha * beta;
        if (requiredBits > 63) {
            throw new IllegalArgumentException(
                    "2*maxResolution + 1 + alpha*beta = " + requiredBits
                            + " exceeds 63 bits; reduce maxResolution or alpha*beta "
                            + "(see the paper's discussion after Eq. 3)");
        }

        this.alpha = alpha;
        this.beta = beta;
        this.g = maxResolution;
        this.bounds = bounds;
        this.numShards = numShards;
        this.greedyStarts = greedyStarts;
        this.greedyWorkBudget = greedyWorkBudget;
        this.randomSeed = randomSeed;
    }

    // =====================================================================
    // Public API
    // =====================================================================

    /**
     * Build the TShape index over a dataset of trajectories.
     *
     * The build process has two passes:
     *
     * Pass 1: compute each trajectory's raw TShape representation and
     * collect the observed raw bitmaps for every enlarged element.
     *
     * Pass 2: after the per-element shape dictionaries have been optimized,
     * encode each trajectory into its final TShape integer value.
     *
     * Profiling
     * ---------
     * Every call records a wall-clock breakdown by phase in
     * lastBuildProfile (seconds per phase, plus "total"). This is always on
     * -- the timer calls themselves cost a negligible fraction of a
     * millisecond compared to the phases being measured -- so callers can
     * always inspect it (or call printBuildProfile()) after build() returns.
     * This is useful because the *bottleneck phase changes with alpha/beta
     * and dataset size*: for small enlarged elements the per-trajectory
     * rasterization (pass 1) dominates, while for large alpha*beta or
     * datasets with many shapes per element, the greedy shape-code
     * optimization can dominate instead (see multiStartGreedyOrder's
     * cost-control comment).
     */
    public void build(Map<String, List<Point>> trajectories) {
        if (trajectories.isEmpty()) {
            return;
        }

        if (bounds == null) {
            bounds = computeGlobalBounds(trajectories);
        }

        List<RawRecord> rawRecords = new ArrayList<>();
        Map<Element, Set<Long>> shapesByElement = new LinkedHashMap<>();

        // --------------------------
        // Pass 1: raw TShape codes
        // --------------------------
        for (Map.Entry<String, List<Point>> entry : trajectories.entrySet()) {
            String tid = entry.getKey();
            List<Point> traj = entry.getValue();
            if (traj.size() < 2) {
                // A single point can be indexed, but for trajectory range
                // queries we usually expect polylines. Skipping keeps the
                // prototype simple.
                continue;
            }

            List<Point> norm = new ArrayList<>(traj.size());
            for (Point p : traj) {
                norm.add(normalizePoint(p));
            }
            Bounds mbr = computeMbr(norm);

            int r = chooseResolution(mbr);
            Element element = findEnlargedElement(mbr, r);
            long rawShape = computeShapeBitmap(norm, element);

            rawRecords.add(new RawRecord(tid, traj, element, rawShape));
            shapesByElement.computeIfAbsent(element, k -> new LinkedHashSet<>()).add(rawShape);
        }

        // Optimize raw shape bitmaps into compact, locality-aware ids. This
        // is usually the most expensive phase when alpha*beta is large --
        // see multiStartGreedyOrder.
        buildShapeDictionaries(shapesByElement);

        // Precompute the BFS bookkeeping used by queryCandidates(): the set
        // of quadtree cells that are either real elements or ancestors of
        // real elements, and the deepest resolution actually in use.
        activePrefixes = buildActivePrefixes();
        maxIndexedResolution = 0;
        for (Element e : shapeDictionary.keySet()) {
            if (e.r > maxIndexedResolution) {
                maxIndexedResolution = e.r;
            }
        }

        // -----------------------------
        // Pass 2: final integer keys
        // -----------------------------
        int bits = alpha * beta;
        for (RawRecord rr : rawRecords) {
            int optId = shapeDictionary.get(rr.element).get(rr.rawShape);
            long qcode = encodeQuadrantCode(rr.element);

            // Eq. 3 in the paper always shifts by the *fixed* width
            // alpha*beta (the size of the raw occupancy bitmap), regardless
            // of how many shapes are actually observed for this particular
            // element. This is what guarantees that two different enlarged
            // elements can never produce the same tshapeValue: each qcode
            // owns a disjoint block of size 2^(alpha*beta).
            //
            // An earlier version of the Python original used a *per-element*
            // bit width (just enough bits for that element's observed shape
            // count). That is more compact in principle, but it is unsafe:
            // qcode << b for a small b can land inside the block reserved
            // for a different qcode << (larger b), causing two unrelated
            // elements to collide on the same integer key and corrupting
            // invertedIndex. Always use the fixed width instead.
            long tshapeValue = (qcode << bits) | (long) optId;

            int shardNum = shard(rr.trajectoryId);
            String rowkey = String.format("%02d:%d:%s", shardNum, tshapeValue, rr.trajectoryId);

            primaryTable.put(rowkey, rr.trajectory);
            invertedIndex.computeIfAbsent(tshapeValue, k -> new ArrayList<>()).add(rr.trajectoryId);

            records.put(
                    rr.trajectoryId,
                    new TShapeRecord(rr.trajectoryId, rr.element, rr.rawShape, optId, qcode, tshapeValue, rowkey));
        }
    }

    /**
     * Return TShape candidate trajectory ids for a spatial range query.
     *
     * This follows the structure of the paper's Algorithm 2: a breadth-first
     * walk of the quadtree, starting from the whole normalized domain and
     * splitting into quadrants, where each visited cell is classified as:
     *
     *   disjoint    -- the cell's alpha x beta enlarged-element footprint
     *                  does not overlap the query rectangle SR at all.
     *                  Nothing in this branch (the cell itself, nor
     *                  anything nested under it) can match, so the whole
     *                  branch is pruned -- we never look at it again.
     *
     *   contains    -- SR fully covers the cell's footprint. If this cell
     *                  happens to be a real indexed element, every one of
     *                  its observed shapes trivially intersects SR (since
     *                  the *whole* footprint does), so we skip the
     *                  per-shape geometry test and take all of them.
     *
     *   intersects  -- SR and the footprint overlap partially. If this cell
     *                  is a real indexed element, each of its observed
     *                  shapes must be tested individually against SR (a
     *                  shape is a strict subset of the footprint, so
     *                  overlap with the footprint does not imply overlap
     *                  with the shape).
     *
     * In every case (except disjoint) we keep recursing into the cell's
     * four children, because -- unlike the paper's idealized construction
     * -- this prototype's enlarged elements are anchored at whichever
     * lower-left cell contains a trajectory's MBR (see
     * findEnlargedElement), not at a fixed non-overlapping alpha x beta
     * grid. That sliding placement means a descendant element's footprint
     * is not guaranteed to nest entirely inside its ancestor's footprint,
     * so "contains" is only used to fast-path the *current* cell, not to
     * assume every element below it is automatically covered too (which is
     * the extra shortcut a real key-value backend gets from the paper's
     * contiguous quadrant-code ranges, Eq. 8).
     *
     * Pruning is what keeps this fast on a large, sparse dataset: the walk
     * only ever visits cells recorded in activePrefixes (real elements and
     * their ancestors -- see buildActivePrefixes), so empty regions of the
     * domain are skipped in O(1) instead of being expanded.
     */
    public Set<String> queryCandidates(Bounds queryRect) {
        if (bounds == null || shapeDictionary.isEmpty()) {
            return new HashSet<>();
        }

        Bounds q = normalizeRect(queryRect);
        Set<String> candidateIds = new HashSet<>();

        // Fixed shift width -- must match the one used when values were
        // written in build(); see the long comment there for why this has
        // to be a *constant*, not something that varies per element.
        int bits = alpha * beta;

        // Start the BFS at the single un-split root cell (resolution 0),
        // which covers the whole normalized [0,1] x [0,1] domain. (The
        // paper's Algorithm 2 pseudocode starts one level lower, at
        // "Root.children" i.e. resolution 1; starting at the true root
        // instead also correctly handles trajectories whose MBR is so large
        // that chooseResolution anchors them at resolution 0 -- a case the
        // paper's own Fig. 5(a) shows as a valid resolution.)
        Deque<Element> queue = new ArrayDeque<>();
        queue.add(new Element(0, 0, 0));

        while (!queue.isEmpty()) {
            Element element = queue.poll();

            // --- Prune empty branches ---------------------------------
            if (!activePrefixes.contains(element)) {
                continue;
            }

            Bounds elementRect = elementRect(element);

            if (!rectsIntersect(elementRect, q)) {
                // disjoint: this branch (and everything nested under it)
                // cannot contribute any candidates.
                continue;
            }

            boolean fullyCovered = rectContains(q, elementRect);

            Map<Integer, Long> reverseMap = reverseShapeDictionary.get(element);
            if (reverseMap != null) {
                // This cell is a real indexed enlarged element (as opposed
                // to a pure ancestor path node with no shapes of its own).
                long qcode = encodeQuadrantCode(element);
                for (Map.Entry<Integer, Long> entry : reverseMap.entrySet()) {
                    int optId = entry.getKey();
                    long rawShape = entry.getValue();
                    if (fullyCovered || shapeIntersectsRect(rawShape, element, q)) {
                        long tshapeValue = (qcode << bits) | (long) optId;
                        List<String> ids = invertedIndex.get(tshapeValue);
                        if (ids != null) {
                            candidateIds.addAll(ids);
                        }
                    }
                }
            }

            // --- Keep descending ----------------------------------------
            // See the method comment above for why we do not skip this
            // even when fullyCovered is true.
            if (element.r < maxIndexedResolution) {
                int cx2 = element.cx * 2;
                int cy2 = element.cy * 2;
                int r2 = element.r + 1;
                queue.add(new Element(cx2, cy2, r2));
                queue.add(new Element(cx2 + 1, cy2, r2));
                queue.add(new Element(cx2, cy2 + 1, r2));
                queue.add(new Element(cx2 + 1, cy2 + 1, r2));
            }
        }

        return candidateIds;
    }

    /** Spatial range query with exact refinement, computing the candidate
     *  set itself. See {@link #exactRangeQuery(Bounds, Set)}. */
    public List<String> exactRangeQuery(Bounds queryRect) {
        return exactRangeQuery(queryRect, null);
    }

    /**
     * Spatial range query with exact refinement.
     *
     * TShape is a filtering index. It can produce false positives because
     * the occupancy bitmap is still an approximation. Therefore, final
     * answers are obtained by checking the original trajectory geometry
     * against the query rectangle.
     *
     * @param candidates optional precomputed result of
     *                   queryCandidates(queryRect). If null, it is computed
     *                   here as usual. Callers that already have the
     *                   candidate set (e.g. benchmarkSpatialQueries(), which
     *                   wants to time the *refinement* step in isolation)
     *                   can pass it in to avoid running the quadtree walk
     *                   twice.
     */
    public List<String> exactRangeQuery(Bounds queryRect, Set<String> candidates) {
        Set<String> cands = candidates != null ? candidates : queryCandidates(queryRect);
        List<String> results = new ArrayList<>();

        for (String tid : cands) {
            List<Point> traj = getTrajectoryById(tid);
            if (traj == null) {
                continue;
            }

            for (int i = 0; i < traj.size() - 1; i++) {
                if (segmentIntersectsRect(traj.get(i), traj.get(i + 1), queryRect)) {
                    results.add(tid);
                    break;
                }
            }
        }

        return results;
    }

    /**
     * Time queryCandidates() and exactRangeQuery() for a batch of query
     * rectangles and report per-query selectivity, mirroring the kind of
     * measurement the paper reports in Section VI (query time and number of
     * candidates as the query window varies).
     *
     * This method is intentionally dataset-agnostic: it has no notion of
     * real-world units (meters, degrees, ...), random sampling strategy, or
     * ground-truth verification. Callers that want those -- e.g.
     * GeolifeExperiment.java, which relates query windows to meters and
     * cross-checks against a brute-force reference -- build them on top of
     * this, so the geo-specific logic doesn't leak into the index itself.
     *
     * @param queryRects query rectangles in the *original* (un-normalized)
     *                   coordinate system -- the same units used when
     *                   build() was called.
     */
    public List<QueryTiming> benchmarkSpatialQueries(List<Bounds> queryRects) {
        List<QueryTiming> rows = new ArrayList<>();
        for (Bounds rect : queryRects) {
            long t0 = System.nanoTime();
            Set<String> candidates = queryCandidates(rect);
            long t1 = System.nanoTime();
            List<String> exact = exactRangeQuery(rect, candidates);
            long t2 = System.nanoTime();

            double candidateTimeS = (t1 - t0) / 1_000_000_000.0;
            double exactTimeS = (t2 - t1) / 1_000_000_000.0;
            rows.add(new QueryTiming(candidates.size(), exact.size(), candidateTimeS, exactTimeS, candidateTimeS + exactTimeS));
        }
        return rows;
    }

    // =====================================================================
    // Discrete Frechet similarity queries
    //
    // The TMan paper notes (Section on similarity queries) that TShape can
    // reuse TraSS's two-stage pruning scheme -- "global pruning" (a cheap,
    // index-level filter that only needs bounding boxes) followed by a
    // "local filter" (a cheap, per-point geometric check) -- "with a slight
    // modification" before the final expensive exact-distance refinement.
    // This is exactly the three-stage pipeline implemented below:
    //
    //   1. global pruning  -- queryCandidates() on the epsilon-expanded MBR
    //                         of the query trajectory (spatial-index lookup,
    //                         no trajectory geometry touched yet), then
    //                         frechetMbrPrune() to tighten that to the exact
    //                         MBR-containment condition Frechet queries need.
    //   2. local filter    -- frechetLocalFilter(): an O(n*m) nearest-point
    //                         scan per surviving candidate, much cheaper
    //                         than the DP below because it needs no table.
    //   3. exact refinement -- discreteFrechet(): the real O(n*m) dynamic
    //                         program, run only on whatever is left.
    //
    // Both pruning stages are *sound* (never reject a true match): this was
    // verified with a random-trajectory stress test (4000 trials, threshold
    // and trajectory lengths/positions all randomized) before this was
    // ported from a throwaway Python prototype into this file -- see the
    // project notes for that script. Soundness rests on one inequality that
    // holds for any two trajectories P, Q:
    //
    //     Hausdorff(P, Q) <= discreteFrechet(P, Q)
    //
    // (Frechet distance requires a *monotonic* coupling between the two
    // sequences; Hausdorff distance is the same "every point of one is near
    // some point of the other" idea without the monotonicity constraint, so
    // it can only be smaller or equal.) That means discreteFrechet(P,Q) <=
    // epsilon implies Hausdorff(P,Q) <= epsilon, which implies:
    //   - every point of P lies within epsilon of *some* point of Q, and
    //     vice versa (this is exactly frechetLocalFilter's check), and
    //   - therefore the MBR of P must lie inside the MBR of Q expanded by
    //     epsilon in every direction, and vice versa (frechetMbrPrune).
    // Both are only necessary conditions, not sufficient ones -- pruning can
    // (and does, per the stress test: ~95% of random pairs) reject true
    // non-matches, but can never reject a true match.
    // =====================================================================

    /**
     * Discrete-Frechet threshold similarity query: return every indexed
     * trajectory whose discrete Frechet distance to queryTrajectory is
     * <= epsilon, together with its points and distance.
     *
     * @param queryTrajectory the query trajectory, in the same
     *                        (un-normalized) coordinate system used when
     *                        build() was called.
     * @param epsilon         the similarity threshold, in the same distance
     *                        units as the trajectory coordinates (e.g. if
     *                        points are (longitude, latitude) in degrees,
     *                        epsilon is in degrees too -- convert meters to
     *                        degrees the same way GeolifeExperiment.java's
     *                        metersToDegrees() does before calling this).
     * @return matches sorted by ascending distance (closest first).
     */
    public List<FrechetMatch> frechetRangeQuery(Point[] queryTrajectory, double epsilon) {
        if (queryTrajectory == null || queryTrajectory.length == 0) {
            return new ArrayList<>();
        }

        List<Point> queryList = Arrays.asList(queryTrajectory);
        Bounds queryMbr = computeMbr(queryList);
        Bounds expanded = new Bounds(
                queryMbr.xmin - epsilon, queryMbr.ymin - epsilon,
                queryMbr.xmax + epsilon, queryMbr.ymax + epsilon);

        // Stage 1a: ask the spatial index which trajectories could possibly
        // overlap the expanded box at all. This reuses the exact same BFS
        // machinery as spatial range queries -- Frechet queries are simply
        // spatial range queries against a derived rectangle, followed by
        // Frechet-specific pruning/refinement instead of the usual
        // segment-intersection refinement.
        Set<String> spatialCandidates = queryCandidates(expanded);

        List<FrechetMatch> matches = new ArrayList<>();
        for (String tid : spatialCandidates) {
            List<Point> candidateTraj = getTrajectoryById(tid);
            if (candidateTraj == null || candidateTraj.isEmpty()) {
                continue;
            }

            // Stage 1b: tighten queryCandidates()'s "intersects" test to the
            // stronger "is contained in" test that Frechet pruning actually
            // needs (see the class-level comment above for why this is
            // sound).
            Bounds candidateMbr = computeMbr(candidateTraj);
            if (!frechetMbrPrune(candidateMbr, expanded)) {
                continue;
            }

            Point[] candidateArr = candidateTraj.toArray(new Point[0]);

            // Stage 2: local filter.
            if (!frechetLocalFilter(candidateArr, queryTrajectory, epsilon)) {
                continue;
            }

            // Stage 3: exact refinement.
            double dist = discreteFrechet(candidateArr, queryTrajectory);
            if (dist <= epsilon) {
                matches.add(new FrechetMatch(tid, candidateTraj, dist));
            }
        }

        matches.sort((a, b) -> Double.compare(a.distance, b.distance));
        return matches;
    }

    /**
     * Exact discrete Frechet distance between two trajectories, computed by
     * the standard dynamic program:
     *
     *   ca(0,0)   = d(p0, q0)
     *   ca(i,0)   = max(ca(i-1,0), d(pi, q0))
     *   ca(0,j)   = max(ca(0,j-1), d(p0, qj))
     *   ca(i,j)   = max(min(ca(i-1,j), ca(i-1,j-1), ca(i,j-1)), d(pi, qj))
     *   result    = ca(n-1, m-1)
     *
     * Implemented with a rolling pair of rows (O(m) memory) instead of the
     * full n x m table, since only the previous row is ever read.
     * O(n*m) time either way.
     */
    public static double discreteFrechet(Point[] p, Point[] q) {
        if (p.length == 0 || q.length == 0) {
            throw new IllegalArgumentException("discreteFrechet requires non-empty trajectories");
        }

        int n = p.length;
        int m = q.length;
        double[] prev = new double[m];
        double[] curr = new double[m];

        for (int j = 0; j < m; j++) {
            double d = euclideanDistance(p[0], q[j]);
            prev[j] = (j == 0) ? d : Math.max(prev[j - 1], d);
        }

        for (int i = 1; i < n; i++) {
            curr[0] = Math.max(prev[0], euclideanDistance(p[i], q[0]));
            for (int j = 1; j < m; j++) {
                double best = Math.min(Math.min(prev[j], prev[j - 1]), curr[j - 1]);
                curr[j] = Math.max(best, euclideanDistance(p[i], q[j]));
            }
            double[] tmp = prev;
            prev = curr;
            curr = tmp;
        }

        return prev[m - 1];
    }

    /**
     * Global-pruning check: is candidateMbr fully contained inside
     * expandedQueryMbr? A necessary (not sufficient) condition for
     * discreteFrechet(candidate, query) <= epsilon when expandedQueryMbr is
     * the query trajectory's MBR expanded by epsilon in every direction --
     * see the class-level comment above for the Hausdorff-distance argument.
     * Exposed as its own method (rather than inlined) so it can be unit
     * tested and reused by the Parquet-backed query (TShapeFrechetQuery.java
     * applies the identical check as a Parquet column-statistics
     * FilterPredicate, letting row groups be skipped without reading them).
     */
    public static boolean frechetMbrPrune(Bounds candidateMbr, Bounds expandedQueryMbr) {
        return rectContains(expandedQueryMbr, candidateMbr);
    }

    /**
     * Local-filter check: is every point of `candidate` within epsilon of
     * some point of `query`, AND every point of `query` within epsilon of
     * some point of `candidate`? A necessary (not sufficient) condition for
     * discreteFrechet(candidate, query) <= epsilon -- see the class-level
     * comment above. O(n*m) but with a much smaller constant than the DP in
     * discreteFrechet() (no table, and it can exit on the first violation).
     */
    public static boolean frechetLocalFilter(Point[] candidate, Point[] query, double epsilon) {
        for (Point c : candidate) {
            if (minDistanceToTrajectory(c, query) > epsilon) {
                return false;
            }
        }
        for (Point q : query) {
            if (minDistanceToTrajectory(q, candidate) > epsilon) {
                return false;
            }
        }
        return true;
    }

    /** Minimum Euclidean distance from a single point to any point of a trajectory. */
    private static double minDistanceToTrajectory(Point p, Point[] trajectory) {
        double best = Double.POSITIVE_INFINITY;
        for (Point t : trajectory) {
            double d = euclideanDistance(p, t);
            if (d < best) {
                best = d;
            }
        }
        return best;
    }

    private static double euclideanDistance(Point a, Point b) {
        double dx = a.x - b.x;
        double dy = a.y - b.y;
        return Math.sqrt(dx * dx + dy * dy);
    }

    /**
     * Return diagnostic statistics for validating the implementation.
     *
     * These statistics are useful because the TMan paper analyzes:
     * - the distribution of selected resolutions;
     * - the number of observed shapes per enlarged element;
     * - the fact that only a small fraction of all 2^(alpha*beta) bitmaps
     *   occur.
     *
     * Returned as a LinkedHashMap<String,Object> (rather than a dedicated
     * class) so it is trivial to dump to JSON/print, mirroring the Python
     * version's plain dict return value.
     */
    public Map<String, Object> stats() {
        int numTrajectories = records.size();
        int numElements = shapeDictionary.size();

        List<Integer> shapesPerElement = new ArrayList<>();
        for (Map<Long, Integer> mapping : shapeDictionary.values()) {
            shapesPerElement.add(mapping.size());
        }

        List<Integer> bitmapCardinalities = new ArrayList<>();
        for (TShapeRecord rec : records.values()) {
            bitmapCardinalities.add(Long.bitCount(rec.rawShape));
        }

        Map<Integer, Integer> resolutionHistogram = new TreeMap<>();
        Map<Element, Integer> elementLoad = new HashMap<>();
        for (TShapeRecord rec : records.values()) {
            resolutionHistogram.merge(rec.element.r, 1, Integer::sum);
            elementLoad.merge(rec.element, 1, Integer::sum);
        }
        List<Integer> trajectoriesPerElement = new ArrayList<>(elementLoad.values());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("num_trajectories", numTrajectories);
        result.put("num_enlarged_elements", numElements);
        result.put("avg_shapes_per_element", mean(shapesPerElement));
        result.put("median_shapes_per_element", median(shapesPerElement));
        result.put("max_shapes_per_element", maxOrZero(shapesPerElement));
        result.put("avg_bitmap_cardinality", mean(bitmapCardinalities));
        result.put("median_bitmap_cardinality", median(bitmapCardinalities));
        result.put("max_bitmap_cardinality", maxOrZero(bitmapCardinalities));
        result.put("avg_trajectories_per_element", mean(trajectoriesPerElement));
        result.put("max_trajectories_per_element", maxOrZero(trajectoriesPerElement));
        result.put("resolution_histogram", resolutionHistogram);
        return result;
    }

    /** Pretty-print the diagnostics returned by stats(). */
    public void printStats() {
        Map<String, Object> s = stats();

        System.out.println("TShape index statistics");
        System.out.println("-----------------------");
        System.out.println("Trajectories: " + s.get("num_trajectories"));
        System.out.println("Enlarged elements: " + s.get("num_enlarged_elements"));
        System.out.println();
        System.out.printf("Avg shapes / element: %.2f%n", (double) s.get("avg_shapes_per_element"));
        System.out.println("Median shapes / element: " + s.get("median_shapes_per_element"));
        System.out.println("Max shapes / element: " + s.get("max_shapes_per_element"));
        System.out.println();
        System.out.printf("Avg bitmap cardinality: %.2f%n", (double) s.get("avg_bitmap_cardinality"));
        System.out.println("Median bitmap cardinality: " + s.get("median_bitmap_cardinality"));
        System.out.println("Max bitmap cardinality: " + s.get("max_bitmap_cardinality"));
        System.out.println();
        System.out.printf("Avg trajectories / element: %.2f%n", (double) s.get("avg_trajectories_per_element"));
        System.out.println("Max trajectories / element: " + s.get("max_trajectories_per_element"));
        System.out.println();
        System.out.println("Resolution histogram:");
        @SuppressWarnings("unchecked")
        Map<Integer, Integer> histogram = (Map<Integer, Integer>) s.get("resolution_histogram");
        for (Map.Entry<Integer, Integer> entry : histogram.entrySet()) {
            System.out.println("  r=" + entry.getKey() + ": " + entry.getValue());
        }
    }

    /**
     * Visualize one trajectory and its selected TShape bitmap, saved as a
     * PNG. Mirrors the Python version's plot_trajectory_tshape(), but uses
     * only java.awt.Graphics2D and javax.imageio -- both part of the
     * standard library -- so, unlike the Python original (which needs
     * matplotlib), this needs no external dependency at all.
     *
     * The image is in normalized [0,1] coordinates. It shows: the
     * trajectory polyline, the selected alpha x beta enlarged element's
     * boundary, the local grid, and the occupied cells (bits set in
     * rawShape).
     */
    public void plotTrajectoryTShape(String tid, String savePath) throws IOException {
        TShapeRecord rec = records.get(tid);
        if (rec == null) {
            throw new IllegalArgumentException("Unknown trajectory id: " + tid);
        }
        List<Point> traj = getTrajectoryById(tid);
        if (traj == null) {
            throw new IllegalStateException("Trajectory data for " + tid + " not found");
        }

        List<Point> norm = new ArrayList<>(traj.size());
        for (Point p : traj) {
            norm.add(normalizePoint(p));
        }

        Element element = rec.element;
        double cell = Math.pow(0.5, element.r);
        double pad = cell * 2;
        double viewXMin = Math.max(0, element.cx * cell - pad);
        double viewXMax = Math.min(1, (element.cx + alpha) * cell + pad);
        double viewYMin = Math.max(0, element.cy * cell - pad);
        double viewYMax = Math.min(1, (element.cy + beta) * cell + pad);

        int size = 800;
        int margin = 60;
        BufferedImage image = new BufferedImage(size, size, BufferedImage.TYPE_INT_RGB);
        Graphics2D g2 = image.createGraphics();
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2.setColor(Color.WHITE);
        g2.fillRect(0, 0, size, size);

        // Map normalized-domain coordinates (within [viewXMin,viewXMax] x
        // [viewYMin,viewYMax]) to pixel coordinates, flipping Y since image
        // rows grow downward while our y axis grows upward.
        double spanX = Math.max(viewXMax - viewXMin, 1e-9);
        double spanY = Math.max(viewYMax - viewYMin, 1e-9);
        int plotSize = size - 2 * margin;

        // Local closure-style coordinate mapper implemented as a small
        // private inline lambda-free helper (kept as plain methods below to
        // avoid capturing too much state in a lambda).

        // Highlight occupied cells first, so the grid/trajectory draw on top.
        for (int dy = 0; dy < beta; dy++) {
            for (int dx = 0; dx < alpha; dx++) {
                int bitId = dy * alpha + dx;
                if ((rec.rawShape & (1L << bitId)) != 0) {
                    Bounds cellR = cellRect(element, dx, dy);
                    int[] px1 = toPixel(cellR.xmin, cellR.ymin, viewXMin, viewYMin, spanX, spanY, margin, plotSize);
                    int[] px2 = toPixel(cellR.xmax, cellR.ymax, viewXMin, viewYMin, spanX, spanY, margin, plotSize);
                    g2.setColor(new Color(255, 165, 0, 90));
                    int rx = Math.min(px1[0], px2[0]);
                    int ry = Math.min(px1[1], px2[1]);
                    int rw = Math.abs(px2[0] - px1[0]);
                    int rh = Math.abs(px2[1] - px1[1]);
                    g2.fillRect(rx, ry, rw, rh);
                }
            }
        }

        // Local alpha x beta grid.
        g2.setColor(new Color(180, 180, 180));
        g2.setStroke(new BasicStroke(1f));
        for (int dx = 0; dx <= alpha; dx++) {
            double x = (element.cx + dx) * cell;
            int[] p1 = toPixel(x, element.cy * cell, viewXMin, viewYMin, spanX, spanY, margin, plotSize);
            int[] p2 = toPixel(x, (element.cy + beta) * cell, viewXMin, viewYMin, spanX, spanY, margin, plotSize);
            g2.drawLine(p1[0], p1[1], p2[0], p2[1]);
        }
        for (int dy = 0; dy <= beta; dy++) {
            double y = (element.cy + dy) * cell;
            int[] p1 = toPixel(element.cx * cell, y, viewXMin, viewYMin, spanX, spanY, margin, plotSize);
            int[] p2 = toPixel((element.cx + alpha) * cell, y, viewXMin, viewYMin, spanX, spanY, margin, plotSize);
            g2.drawLine(p1[0], p1[1], p2[0], p2[1]);
        }

        // Enlarged element boundary (thicker).
        g2.setColor(Color.BLACK);
        g2.setStroke(new BasicStroke(2.5f));
        int[] eLL = toPixel(element.cx * cell, element.cy * cell, viewXMin, viewYMin, spanX, spanY, margin, plotSize);
        int[] eUR = toPixel((element.cx + alpha) * cell, (element.cy + beta) * cell, viewXMin, viewYMin, spanX, spanY, margin, plotSize);
        g2.drawRect(Math.min(eLL[0], eUR[0]), Math.min(eLL[1], eUR[1]), Math.abs(eUR[0] - eLL[0]), Math.abs(eUR[1] - eLL[1]));

        // Trajectory polyline + point markers.
        g2.setColor(new Color(31, 119, 180));
        g2.setStroke(new BasicStroke(2f));
        int[] prev = null;
        for (Point p : norm) {
            int[] px = toPixel(p.x, p.y, viewXMin, viewYMin, spanX, spanY, margin, plotSize);
            if (prev != null) {
                g2.drawLine(prev[0], prev[1], px[0], px[1]);
            }
            g2.fillOval(px[0] - 3, px[1] - 3, 6, 6);
            prev = px;
        }

        // Title.
        g2.setColor(Color.BLACK);
        Font font = new Font("SansSerif", Font.PLAIN, 14);
        g2.setFont(font);
        FontMetrics fm = g2.getFontMetrics();
        String title = tid + ": element=" + element + ", rawShape=" + Long.toBinaryString(rec.rawShape)
                + ", optId=" + rec.optimizedShapeId;
        g2.drawString(title, (size - fm.stringWidth(title)) / 2, 24);

        g2.dispose();

        File outFile = new File(savePath);
        File parent = outFile.getParentFile();
        if (parent != null) {
            parent.mkdirs();
        }
        ImageIO.write(image, "png", outFile);
        System.out.println("Saved plot to " + savePath);
    }

    /** Map a normalized-domain (x, y) point into pixel coordinates for
     *  plotTrajectoryTShape(), flipping the Y axis (image rows grow
     *  downward). Returns {pixelX, pixelY}. */
    private static int[] toPixel(
            double x, double y, double viewXMin, double viewYMin, double spanX, double spanY, int margin, int plotSize) {
        int px = margin + (int) Math.round((x - viewXMin) / spanX * plotSize);
        int py = margin + plotSize - (int) Math.round((y - viewYMin) / spanY * plotSize);
        return new int[] {px, py};
    }

    // =====================================================================
    // Shape dictionary construction and optimization
    // =====================================================================

    /**
     * Build the per-enlarged-element mapping from raw bitmaps to optimized
     * ids.
     *
     * In the paper, only observed shapes are encoded, and their ids are
     * assigned so that similar shapes have nearby values. This mapping is
     * cached in Redis in TMan. Here it is stored in plain maps.
     */
    private void buildShapeDictionaries(Map<Element, Set<Long>> shapesByElement) {
        Random rng = new Random(randomSeed);

        for (Map.Entry<Element, Set<Long>> entry : shapesByElement.entrySet()) {
            Element element = entry.getKey();
            List<Long> shapes = new ArrayList<>(entry.getValue());

            List<Long> ordered;
            if (shapes.size() == 1) {
                ordered = shapes;
            } else {
                ordered = multiStartGreedyOrder(shapes, rng);
            }

            Map<Long, Integer> fwd = new LinkedHashMap<>();
            Map<Integer, Long> rev = new LinkedHashMap<>();
            for (int i = 0; i < ordered.size(); i++) {
                fwd.put(ordered.get(i), i);
                rev.put(i, ordered.get(i));
            }
            shapeDictionary.put(element, fwd);
            reverseShapeDictionary.put(element, rev);
        }
    }

    /**
     * Approximate the paper's greedy TSP-style shape ordering.
     *
     * The paper wants an ordering that maximizes adjacent Jaccard
     * similarity: sum_i Jaccard(shape_i, shape_{i+1}). Equivalently, this
     * minimizes adjacent distance: sum_i (1 - Jaccard(shape_i, shape_{i+1})).
     *
     * The exact implementation of the paper's greedy method is not
     * specified. This version uses multiple random starts and keeps the
     * best greedy path.
     *
     * Cost control
     * ------------
     * Building one greedy path from one start costs O(M^2) pairwise Jaccard
     * comparisons, where M = shapes.size(). Trying greedyStarts different
     * starting points multiplies that by greedyStarts. That is fine for
     * elements with a handful of shapes, but the paper itself reports
     * enlarged elements with thousands of *observed* shapes in practice
     * (Fig. 16(a): up to ~4700 for 5x5 cells). To keep worst-case cost
     * bounded regardless of M, we scale down the number of starts so that
     * (effectiveStarts * M^2) stays under greedyWorkBudget. Small elements
     * still get the full greedyStarts passes; very large elements
     * automatically fall back to fewer passes, down to a single greedy
     * pass, rather than stalling the build.
     */
    private List<Long> multiStartGreedyOrder(List<Long> shapes, Random rng) {
        if (shapes.size() <= 1) {
            return shapes;
        }

        long m = shapes.size();
        long budgetStarts = Math.max(1L, greedyWorkBudget / (m * m));
        int effectiveStarts = (int) Math.max(1L, Math.min((long) greedyStarts, budgetStarts));

        List<Long> starts = new ArrayList<>(shapes);
        Collections.shuffle(starts, rng);
        if (starts.size() > effectiveStarts) {
            starts = new ArrayList<>(starts.subList(0, effectiveStarts));
        }

        List<Long> bestOrder = null;
        double bestScore = Double.POSITIVE_INFINITY;

        for (long start : starts) {
            List<Long> order = greedyOrderFromStart(shapes, start);
            double score = totalDistance(order);
            if (score < bestScore) {
                bestScore = score;
                bestOrder = order;
            }
        }

        return bestOrder != null ? bestOrder : shapes;
    }

    /** Build one nearest-neighbor greedy path in shape space. */
    private List<Long> greedyOrderFromStart(List<Long> shapes, long start) {
        LinkedHashSet<Long> remaining = new LinkedHashSet<>(shapes);
        remaining.remove(start);

        List<Long> order = new ArrayList<>();
        order.add(start);
        long current = start;

        while (!remaining.isEmpty()) {
            long best = Long.MIN_VALUE;
            double bestDist = Double.POSITIVE_INFINITY;
            boolean first = true;
            for (long candidate : remaining) {
                double dist = 1.0 - jaccard(current, candidate);
                // Tie-break on the smaller shape value, matching the
                // Python version's `key=lambda s: (1.0 - jaccard(current, s), s)`.
                if (first || dist < bestDist || (dist == bestDist && candidate < best)) {
                    best = candidate;
                    bestDist = dist;
                    first = false;
                }
            }
            order.add(best);
            remaining.remove(best);
            current = best;
        }

        return order;
    }

    /** Jaccard similarity between two bitmap-encoded shapes. */
    private static double jaccard(long a, long b) {
        int inter = Long.bitCount(a & b);
        int union = Long.bitCount(a | b);
        return union != 0 ? (double) inter / union : 0.0;
    }

    /** Total adjacent Jaccard distance for a candidate shape ordering. */
    private static double totalDistance(List<Long> order) {
        double sum = 0.0;
        for (int i = 0; i < order.size() - 1; i++) {
            sum += 1.0 - jaccard(order.get(i), order.get(i + 1));
        }
        return sum;
    }

    // =====================================================================
    // TShape construction primitives
    // =====================================================================

    /**
     * Select the finest resolution whose alpha x beta enlarged element can
     * contain the trajectory MBR.
     *
     * This follows the logic of Lemmas 3 and 4 in the paper: first compute
     * a candidate resolution from the MBR width/height, then check whether
     * the MBR actually fits into the alpha x beta window starting at the
     * lower-left cell containing the MBR lower-left corner; if not, move
     * one level coarser.
     */
    private int chooseResolution(Bounds mbr) {
        double width = Math.max(mbr.xmax - mbr.xmin, 1e-15);
        double height = Math.max(mbr.ymax - mbr.ymin, 1e-15);

        double value = Math.max(width / alpha, height / beta);
        int l = value > 0 ? (int) Math.floor(Math.log(value) / Math.log(0.5)) : g;
        l = Math.max(0, Math.min(l, g));

        double cell = Math.pow(0.5, l);
        double ex = Math.floor(mbr.xmin / cell) * cell;
        double ey = Math.floor(mbr.ymin / cell) * cell;

        if (ex + alpha * cell >= mbr.xmax && ey + beta * cell >= mbr.ymax) {
            return l;
        }
        return Math.max(0, l - 1);
    }

    /**
     * Return the enlarged element that covers the trajectory MBR at
     * resolution r.
     *
     * The element is identified by the lower-left cell of the alpha x beta
     * window. The paper describes the element by the quadrant sequence of
     * this lower-left cell; this implementation stores integer grid
     * coordinates and derives the quadrant code when needed
     * (encodeQuadrantCode).
     */
    private Element findEnlargedElement(Bounds mbr, int r) {
        double cell = Math.pow(0.5, r);
        long gridSize = 1L << r;

        long cx = clampLong((long) Math.floor(mbr.xmin / cell), 0, gridSize - 1);
        long cy = clampLong((long) Math.floor(mbr.ymin / cell), 0, gridSize - 1);

        // Clamp so the alpha x beta window stays inside the normalized domain.
        long maxCx = Math.max(0, gridSize - alpha);
        long maxCy = Math.max(0, gridSize - beta);
        cx = Math.min(cx, maxCx);
        cy = Math.min(cy, maxCy);

        return new Element((int) cx, (int) cy, r);
    }

    private static long clampLong(long v, long lo, long hi) {
        return Math.min(Math.max(v, lo), hi);
    }

    /**
     * Rasterize a trajectory into an alpha x beta occupancy bitmap.
     *
     * Bit layout: bitId = dy * alpha + dx, where dx and dy are local cell
     * coordinates inside the enlarged element.
     *
     * NOTE: The paper does not specify the exact rasterizer. This
     * implementation marks a cell as occupied iff at least one trajectory
     * segment intersects that cell. This is conservative for range
     * filtering: it avoids false negatives due to missed crossed cells.
     */
    private long computeShapeBitmap(List<Point> normTraj, Element element) {
        double cell = Math.pow(0.5, element.r);
        long bitmap = 0L;

        for (int i = 0; i < normTraj.size() - 1; i++) {
            Point p1 = normTraj.get(i);
            Point p2 = normTraj.get(i + 1);

            // Restrict intersection tests to cells touched by the segment MBR.
            double sxmin = Math.min(p1.x, p2.x);
            double sxmax = Math.max(p1.x, p2.x);
            double symin = Math.min(p1.y, p2.y);
            double symax = Math.max(p1.y, p2.y);

            int dx0 = Math.max(0, (int) Math.floor((sxmin - element.cx * cell) / cell));
            int dx1 = Math.min(alpha - 1, (int) Math.floor((sxmax - element.cx * cell) / cell));
            int dy0 = Math.max(0, (int) Math.floor((symin - element.cy * cell) / cell));
            int dy1 = Math.min(beta - 1, (int) Math.floor((symax - element.cy * cell) / cell));

            for (int dx = dx0; dx <= dx1; dx++) {
                for (int dy = dy0; dy <= dy1; dy++) {
                    Bounds rect = cellRect(element, dx, dy);
                    if (segmentIntersectsRect(p1, p2, rect)) {
                        int bitId = dy * alpha + dx;
                        bitmap |= (1L << bitId);
                    }
                }
            }
        }

        return bitmap;
    }

    /**
     * Encode the lower-left cell of an enlarged element as a quadtree code,
     * following Equation 2 of the paper:
     *
     *     code(Q) = sum_{i=1..r} ( q_i * (4^(g-i+1) - 1) / 3 + 1 ) - 1
     *
     * where Q = q1 q2 ... qr is the quadrant-sequence path of the cell
     * (q1 = coarsest split, qr = finest split, one of {0,1,2,3} per level),
     * r is this element's own resolution, and g = this.g is the *global*
     * maximum resolution used anywhere in the index.
     *
     * Quadrant numbering used here (matches the paper's Fig. 2/8 layout):
     *
     *     2 3
     *     0 1
     *
     * Why the "-i+1" term (and thus dependence on the *global* g) matters
     * -----------------------------------------------------------------
     * A naive path-to-integer encoding (e.g. treating q1 q2 ... qr as a
     * base-4 number) would assign resolution-1 cells the codes 0..3,
     * resolution-2 cells 4..19, resolution-3 cells 20..83, and so on: every
     * resolution occupies its own separate, non-overlapping block of the
     * integer line. That is internally consistent (no two elements
     * collide) but it is *useless* for range queries: a coarse cell's code
     * ends up nowhere near the codes of its own children, because children
     * of *every* resolution-2 cell are interleaved together in one big
     * block instead of sitting right after their own parent.
     *
     * Equation 2 avoids this by making each level's contribution scale
     * with (g - i + 1): the coefficient for the *first* (coarsest) digit is
     * the largest (approximately 4^g / 3), reserving a big enough "slot"
     * below it for every possible descendant down to the deepest
     * resolution g. The result is a proper depth-first pre-order traversal
     * of the whole quadtree up to depth g: a cell's code, and the codes of
     * everything nested under it, form one contiguous run of integers.
     * That is exactly the property Algorithm 2 in the paper relies on
     * (Eq. 8: "if SR covers Ei, every Ej prefixed by Ei is covered too" --
     * expressed as a single contiguous integer interval
     * [code(Ei), code(Ei)+EN(Ei))).
     */
    private long encodeQuadrantCode(Element element) {
        if (element.r == 0) {
            // The single un-split cell covering the whole normalized
            // domain. Equation 2's sum is empty for r=0 (no quadrant digits
            // at all); we special-case it to a fixed constant (0) rather
            // than the formula's literal "-1", purely so this element's
            // code is a non-negative number like every other element's.
            return 0L;
        }

        long code = 0L;
        for (int level = 1; level <= element.r; level++) {
            // `level` walks from the coarsest split (1) to this element's
            // own resolution (r). `shift` picks out the corresponding bit
            // of the integer cell coordinates cx, cy: the most-significant
            // bit of an r-bit coordinate is the level-1 digit, the
            // least-significant bit is the level-r digit.
            int shift = element.r - level;
            int xBit = (element.cx >> shift) & 1;
            int yBit = (element.cy >> shift) & 1;
            int q = 2 * yBit + xBit; // quadrant digit q_i in {0,1,2,3}

            // Number of *leaf slots* reserved for one unit of this digit at
            // this depth: every deeper level (down to g) doubles twice,
            // i.e. multiplies by 4, so the slot size is 4^(g-level+1)
            // cells; dividing (4^k - 1) by 3 turns a geometric sum
            // (1+4+16+...) into a plain count.
            long weight = (pow4(g - level + 1) - 1) / 3;
            code += q * weight + 1;
        }

        return code - 1;
    }

    /** 4^exp as a long, computed as 2^(2*exp) via a bit shift. */
    private static long pow4(int exp) {
        return 1L << (2 * exp);
    }

    // =====================================================================
    // BFS query support
    // =====================================================================

    /**
     * Compute the set of quadtree cells worth visiting during a
     * queryCandidates() BFS: every real indexed element, plus every
     * ancestor cell above it up to the root.
     *
     * Why this is needed
     * -------------------
     * queryCandidates() walks the quadtree top-down (resolution 1, 2, 3,
     * ...), the same way Algorithm 2 in the paper does. But most of that
     * quadtree is empty: trajectories only ever populate a small number of
     * (cx, cy, r) cells out of the 4^r possible ones at resolution r. If
     * the BFS blindly expanded every cell's four children, it would visit
     * up to 4^maxResolution nodes -- billions, for maxResolution=16.
     *
     * By precomputing which cells are "on the path" to at least one real
     * element, the BFS can check membership in this set (an O(1) hash
     * lookup) before deciding whether to keep descending, so the total
     * number of nodes visited is bounded by (number of elements) * (their
     * average depth) instead of exploding with maxResolution.
     *
     * Implementation
     * --------------
     * For each real element (ecx, ecy, er), walk from its own resolution er
     * up to the root (resolution 0), marking each ancestor cell as active.
     * Coordinates shrink by one right-shift per level, since going from
     * resolution r to resolution r-1 halves the grid in each dimension. As
     * soon as an ancestor is already marked (because a previous element
     * shares that prefix), every cell above it must already be marked too,
     * so we can stop climbing early.
     */
    private Set<Element> buildActivePrefixes() {
        Set<Element> active = new HashSet<>();

        for (Element e : shapeDictionary.keySet()) {
            int cx = e.cx;
            int cy = e.cy;
            int r = e.r;
            while (true) {
                Element node = new Element(cx, cy, r);
                if (active.contains(node)) {
                    break; // this prefix (and everything above it) is already recorded
                }
                active.add(node);
                if (r == 0) {
                    break;
                }
                cx >>= 1;
                cy >>= 1;
                r -= 1;
            }
        }

        return active;
    }

    // =====================================================================
    // Query helper methods
    // =====================================================================

    /** Return true if any occupied cell of shape overlaps rect. */
    private boolean shapeIntersectsRect(long shape, Element element, Bounds rect) {
        for (int dy = 0; dy < beta; dy++) {
            for (int dx = 0; dx < alpha; dx++) {
                int bitId = dy * alpha + dx;
                if ((shape & (1L << bitId)) == 0) {
                    continue;
                }
                Bounds cellR = cellRect(element, dx, dy);
                if (rectsIntersect(cellR, rect)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Return the normalized (xmin, ymin, xmax, ymax) footprint of the
     *  alpha x beta enlarged element. */
    private Bounds elementRect(Element element) {
        double cell = Math.pow(0.5, element.r);
        return new Bounds(
                element.cx * cell, element.cy * cell, (element.cx + alpha) * cell, (element.cy + beta) * cell);
    }

    /** Return the normalized rectangle of one local cell in an enlarged
     *  element. */
    private Bounds cellRect(Element element, int dx, int dy) {
        double cell = Math.pow(0.5, element.r);
        return new Bounds(
                (element.cx + dx) * cell,
                (element.cy + dy) * cell,
                (element.cx + dx + 1) * cell,
                (element.cy + dy + 1) * cell);
    }

    // =====================================================================
    // Normalization and dataset utilities
    // =====================================================================

    /** Compute dataset-wide bounds used to normalize coordinates. */
    private static Bounds computeGlobalBounds(Map<String, List<Point>> trajectories) {
        double xmin = Double.POSITIVE_INFINITY;
        double ymin = Double.POSITIVE_INFINITY;
        double xmax = Double.NEGATIVE_INFINITY;
        double ymax = Double.NEGATIVE_INFINITY;
        boolean any = false;

        for (List<Point> traj : trajectories.values()) {
            for (Point p : traj) {
                any = true;
                xmin = Math.min(xmin, p.x);
                ymin = Math.min(ymin, p.y);
                xmax = Math.max(xmax, p.x);
                ymax = Math.max(ymax, p.y);
            }
        }

        if (!any) {
            throw new IllegalArgumentException("Cannot compute bounds from empty trajectories");
        }
        return new Bounds(xmin, ymin, xmax, ymax);
    }

    /** Normalize one point to [0,1] x [0,1]. */
    private Point normalizePoint(Point p) {
        double nx = bounds.xmax > bounds.xmin ? (p.x - bounds.xmin) / (bounds.xmax - bounds.xmin) : 0.0;
        double ny = bounds.ymax > bounds.ymin ? (p.y - bounds.ymin) / (bounds.ymax - bounds.ymin) : 0.0;

        // Use 1 - epsilon so points exactly at xmax/ymax remain inside the
        // last grid cell rather than falling just outside due to floor
        // division.
        double cx = Math.min(Math.max(nx, 0.0), 1.0 - 1e-15);
        double cy = Math.min(Math.max(ny, 0.0), 1.0 - 1e-15);
        return new Point(cx, cy);
    }

    /** Normalize an axis-aligned query rectangle to [0,1] x [0,1]. */
    private Bounds normalizeRect(Bounds rect) {
        double nx1 = bounds.xmax > bounds.xmin ? (rect.xmin - bounds.xmin) / (bounds.xmax - bounds.xmin) : 0.0;
        double nx2 = bounds.xmax > bounds.xmin ? (rect.xmax - bounds.xmin) / (bounds.xmax - bounds.xmin) : 0.0;
        double ny1 = bounds.ymax > bounds.ymin ? (rect.ymin - bounds.ymin) / (bounds.ymax - bounds.ymin) : 0.0;
        double ny2 = bounds.ymax > bounds.ymin ? (rect.ymax - bounds.ymin) / (bounds.ymax - bounds.ymin) : 0.0;

        double a = Math.min(nx1, nx2);
        double b = Math.max(nx1, nx2);
        double c = Math.min(ny1, ny2);
        double d = Math.max(ny1, ny2);

        return new Bounds(
                Math.min(Math.max(a, 0.0), 1.0),
                Math.min(Math.max(c, 0.0), 1.0),
                Math.min(Math.max(b, 0.0), 1.0),
                Math.min(Math.max(d, 0.0), 1.0));
    }

    /** Compute the minimum bounding rectangle of a trajectory. */
    private static Bounds computeMbr(List<Point> traj) {
        double xmin = Double.POSITIVE_INFINITY;
        double ymin = Double.POSITIVE_INFINITY;
        double xmax = Double.NEGATIVE_INFINITY;
        double ymax = Double.NEGATIVE_INFINITY;
        for (Point p : traj) {
            xmin = Math.min(xmin, p.x);
            ymin = Math.min(ymin, p.y);
            xmax = Math.max(xmax, p.x);
            ymax = Math.max(ymax, p.y);
        }
        return new Bounds(xmin, ymin, xmax, ymax);
    }

    // =====================================================================
    // Storage helpers
    // =====================================================================

    /** Retrieve the original trajectory from the toy primary table. */
    private List<Point> getTrajectoryById(String tid) {
        TShapeRecord record = records.get(tid);
        if (record == null) {
            return null;
        }
        return primaryTable.get(record.rowkey);
    }

    /** Hash a trajectory id into a toy shard prefix, using MD5 the same way
     *  the Python version does (int(md5_hex, 16) % numShards), via
     *  BigInteger so we get the same unsigned big-integer semantics as
     *  Python's int() on a hex string. */
    private int shard(String trajectoryId) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(trajectoryId.getBytes(StandardCharsets.UTF_8));
            BigInteger h = new BigInteger(1, digest); // 1 = treat bytes as unsigned/positive
            return h.mod(BigInteger.valueOf(numShards)).intValue();
        } catch (NoSuchAlgorithmException e) {
            // MD5 is guaranteed to be available on every standard JRE, so
            // this should never happen; wrap it so callers don't need to
            // handle a checked exception for an effectively impossible case.
            throw new RuntimeException(e);
        }
    }

    // =====================================================================
    // Small statistics helpers (Java has no stdlib mean/median for List<Integer>)
    // =====================================================================

    private static double mean(List<Integer> values) {
        if (values.isEmpty()) {
            return 0.0;
        }
        long sum = 0;
        for (int v : values) {
            sum += v;
        }
        return (double) sum / values.size();
    }

    private static double median(List<Integer> values) {
        if (values.isEmpty()) {
            return 0.0;
        }
        List<Integer> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        if (n % 2 == 1) {
            return sorted.get(n / 2);
        }
        return (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    }

    private static int maxOrZero(List<Integer> values) {
        return values.isEmpty() ? 0 : Collections.max(values);
    }

    // =====================================================================
    // Geometry predicates
    // =====================================================================

    /** Axis-aligned rectangle intersection, including boundary contact. */
    private static boolean rectsIntersect(Bounds a, Bounds b) {
        return a.xmin <= b.xmax && a.xmax >= b.xmin && a.ymin <= b.ymax && a.ymax >= b.ymin;
    }

    /** Return true if `outer` fully covers `inner` (used for the paper's
     *  "SR contains E" case in Algorithm 2 / spatial range queries). */
    private static boolean rectContains(Bounds outer, Bounds inner) {
        return outer.xmin <= inner.xmin && outer.ymin <= inner.ymin && outer.xmax >= inner.xmax && outer.ymax >= inner.ymax;
    }

    /** Return true if p lies inside or on the boundary of rect. */
    private static boolean pointInRect(Point p, Bounds rect) {
        return rect.xmin <= p.x && p.x <= rect.xmax && rect.ymin <= p.y && p.y <= rect.ymax;
    }

    /** Return true if a segment intersects an axis-aligned rectangle.
     *  Package-visible (not private) so GeolifeExperiment.java can reuse it
     *  as an independent brute-force ground-truth check. */
    static boolean segmentIntersectsRect(Point p1, Point p2, Bounds rect) {
        if (pointInRect(p1, rect) || pointInRect(p2, rect)) {
            return true;
        }

        Point[] corners = {
            new Point(rect.xmin, rect.ymin),
            new Point(rect.xmax, rect.ymin),
            new Point(rect.xmax, rect.ymax),
            new Point(rect.xmin, rect.ymax)
        };
        Point[][] edges = {
            {corners[0], corners[1]},
            {corners[1], corners[2]},
            {corners[2], corners[3]},
            {corners[3], corners[0]}
        };

        for (Point[] edge : edges) {
            if (segmentsIntersect(p1, p2, edge[0], edge[1])) {
                return true;
            }
        }
        return false;
    }

    /** Robust-enough 2D segment intersection predicate for this prototype. */
    private static boolean segmentsIntersect(Point a, Point b, Point c, Point d) {
        double eps = 1e-15;

        double o1 = orient(a, b, c);
        double o2 = orient(a, b, d);
        double o3 = orient(c, d, a);
        double o4 = orient(c, d, b);

        if (Math.abs(o1) <= eps && onSegment(a, c, b)) {
            return true;
        }
        if (Math.abs(o2) <= eps && onSegment(a, d, b)) {
            return true;
        }
        if (Math.abs(o3) <= eps && onSegment(c, a, d)) {
            return true;
        }
        if (Math.abs(o4) <= eps && onSegment(c, b, d)) {
            return true;
        }

        return (o1 > 0) != (o2 > 0) && (o3 > 0) != (o4 > 0);
    }

    private static double orient(Point p, Point q, Point r) {
        return (q.x - p.x) * (r.y - p.y) - (q.y - p.y) * (r.x - p.x);
    }

    private static boolean onSegment(Point p, Point q, Point r) {
        double eps = 1e-15;
        return Math.min(p.x, r.x) - eps <= q.x
                && q.x <= Math.max(p.x, r.x) + eps
                && Math.min(p.y, r.y) - eps <= q.y
                && q.y <= Math.max(p.y, r.y) + eps;
    }

    // =====================================================================
    // Internal record type used only inside build() to carry a trajectory
    // through pass 1 into pass 2 (mirrors the Python version's raw_records
    // list of tuples).
    // =====================================================================

    private static final class RawRecord {
        final String trajectoryId;
        final List<Point> trajectory;
        final Element element;
        final long rawShape;

        RawRecord(String trajectoryId, List<Point> trajectory, Element element, long rawShape) {
            this.trajectoryId = trajectoryId;
            this.trajectory = trajectory;
            this.element = element;
            this.rawShape = rawShape;
        }
    }

    // =====================================================================
    // Small smoke test (mirrors the `if __name__ == "__main__":` block at
    // the bottom of Tshape.py)
    // =====================================================================

    public static void main(String[] args) throws IOException {
        Map<String, List<Point>> trajectories = new LinkedHashMap<>();
        String line;

        BufferedReader br = new BufferedReader(new FileReader("./data/trajectories.txt"));
        while ((line = br.readLine()) != null) {
            int pointsCount = countPoints(line);
            List<Point> trajectory = new ArrayList<>(pointsCount);

            char[] chars = line.toCharArray();
            int ichar = 0;
            int idx = 0;
            int len = chars.length;

            while (ichar < len) {
                // Parse longitude
                int start = ichar;
                while (chars[ichar] != ',') ichar++;
                double lon = parseDouble(chars, start, ichar);
                ichar++; // skip ','

                // Parse latitude
                start = ichar;
                while (chars[ichar] != ',') ichar++;
                double lat = parseDouble(chars, start, ichar);
                ichar++; // skip ','

                // Parse timestamp
                start = ichar;
                while (chars[ichar] != ';') ichar++;
                long tstp = parseLong(chars, start, ichar);
                ichar++; // skip ';'

                trajectory.add(new Point(lon, lat));
            }
            trajectories.put(RandomStringUtils.randomAlphanumeric(17).toUpperCase(),trajectory);
        }

//        trajectories.put("t1", Arrays.asList(new Point(0.0, 0.0), new Point(1.0, 1.0), new Point(2.0, 1.0)));
//        trajectories.put("t2", Arrays.asList(new Point(5.0, 5.0), new Point(6.0, 5.0), new Point(7.0, 6.0)));
//        trajectories.put("t3", Arrays.asList(new Point(0.0, 5.0), new Point(1.0, 4.0), new Point(2.0, 3.0)));
//        trajectories.put("t4", Arrays.asList(new Point(1.0, 0.0), new Point(1.2, 0.5), new Point(1.7, 1.5)));

        TShapeIndex index = new TShapeIndex(3, 3, 8, null, 16, 16, 2_000_000L, 7L);
        index.build(trajectories);
        index.printStats();

        Bounds query = new Bounds(0.5, 0.5, 2.1, 1.6);
        System.out.println();
        System.out.println("Candidate ids: " + index.queryCandidates(query));
        System.out.println("Exact result ids: " + index.exactRangeQuery(query));

        // Discrete-Frechet similarity query: this query trajectory is a
        // near-copy of t1 shifted by ~0.14 units, so at epsilon=0.5 only t1
        // should come back (t4, the next closest, is ~0.91 units away).


        for (Map.Entry<String, List<Point>> entry : trajectories.entrySet()) {
            Point[] frechetQuery = entry.getValue().toArray(new Point[0]);

            for (FrechetMatch match : index.frechetRangeQuery(frechetQuery, 0.2)) {
                System.out.println("Frechet match: " + match);
            }
            System.out.println("OK");
        }

//        Point[] frechetQuery = {new Point(0.1, 0.1), new Point(1.1, 1.1), new Point(2.0, 1.0)};
//        System.out.println();
//        for (FrechetMatch match : index.frechetRangeQuery(frechetQuery, 0.5)) {
//            System.out.println("Frechet match: " + match);
//        }

        // Uncomment to also render a PNG (needs a writable "." directory):
        // index.plotTrajectoryTShape("t1", "./t1_tshape.png");
    }
    private static int countPoints(String line) {
        int count = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == ';') count++;
        }
        return count;
    }

    private static double parseDouble(char[] chars, int start, int end) {
        return Double.parseDouble(new String(chars, start, end - start));
    }

    // Parse a long directly from char[] between start (inclusive) and end (exclusive)
    private static long parseLong(char[] chars, int start, int end) {
        long result = 0;
        boolean neg = false;
        int i = start;
        if (chars[i] == '-') { neg = true; i++; }

        while (i < end && chars[i] >= '0' && chars[i] <= '9') {
            result = result * 10 + (chars[i] - '0');
            i++;
        }

        return neg ? -result : result;
    }
}
