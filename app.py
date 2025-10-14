# app.py
from flask import Flask, jsonify, abort, send_file, request, abort
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    # Normalize old 'postgres://' scheme to 'postgresql://'
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(
        db_url,
        pool_pre_ping=True,
    )
    return _engine

def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")
    
    # --- Dissociate by TERMS: "A but not B" ---
    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_terms(term_a: str, term_b: str):
        """
        Query ns.metadata.title by substring:
        - include titles containing term_a
        - exclude titles containing term_b
        URL underscores are treated as spaces (e.g., posterior_cingulate)
        /dissociate/terms/posterior_cingulate/ventromedial_prefrontal
        """

        def norm(s: str) -> str:
            # Convert underscores to spaces to make URLs human-friendly
            return s.replace("_", " ").strip()
        
        # Case-insensitive substring patterns for SQL ILIKE
        ta = norm(term_a)
        tb = norm(term_b)

        pat_a = f"%{ta}%"
        pat_b = f"%{tb}%"

        # Optional ?limit= for controlling the result size (default 1000, clamped to [1, 5000])
        try:
            limit = int(request.args.get("limit", "1000"))
            if limit <= 0 or limit > 5000:
                limit = 1000
        except Exception:
            limit = 1000

        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            rows = conn.execute(
                text(f"""
                    SELECT study_id, title, journal, year
                    FROM ns.metadata
                    WHERE title ILIKE :pat_a       -- contains A
                      AND title NOT ILIKE :pat_b   -- but NOT B
                    ORDER BY study_id
                    LIMIT :lim
                """),
                {"pat_a": pat_a, "pat_b": pat_b, "lim": limit},
            ).mappings().all()

        results = [dict(r) for r in rows]

        # Human-friendly JSON with a short summary and parameters echoed back
        return jsonify({
            "ok": True,
            "mode": "title_contains_a_not_b",
            "params": {"term_a": term_a, "term_b": term_b},
            "summary": f'Titles containing "{ta}" but not "{tb}"',
            "count": len(results),
            "studies": results
        })
    
    # --- Dissociate by LOCATIONS: "A(x1,y1,z1) but not B(x2,y2,z2)" ---
    @app.get("/dissociate/locations/<coords1>/<coords2>", endpoint="dissociate_locations")
    def dissociate_locations(coords1: str, coords2: str):
        """
        Geometric A\B in 3D space using PostGIS:
        - A: studies whose coordinates are within r_in of coords1
        - B: studies whose coordinates are within r_out of coords2
        Return A minus B.
        coords1/coords2 format: "x_y_z" (floats allowed). Example: 0_-52_26
        Optional query params: ?r_in=2&r_out=2  (same unit as your coordinates)
        """

        def parse_xyz(s: str):
            # Parse "x_y_z" into floats; raise 400 on bad input
            x, y, z = [float(tok) for tok in s.split("_")]
            return {"x": x, "y": y, "z": z}

        c1 = parse_xyz(coords1)
        c2 = parse_xyz(coords2)

        # Radii (defaults). We keep them as floats to match PostGIS distance
        try:
            r_in  = float(request.args.get("r_in", "2"))
            r_out = float(request.args.get("r_out", "2"))
        except Exception:
            r_in, r_out = 2.0, 2.0

        eng = get_engine()
        with eng.begin() as conn:
            # Make sure we read from the ns schema
            conn.execute(text("SET search_path TO ns, public;"))

            # Use ST_3DDistance against geom (POINTZ).  We set SRID on the
            # query points to match the table SRID to avoid mixed-SRID errors
            q = text("""
                WITH a AS (
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE geom IS NOT NULL
                    AND ST_3DDistance(
                            geom,
                            ST_SetSRID(ST_MakePoint(:x1,:y1,:z1), ST_SRID(geom))
                        ) <= :rin
                ),
                b AS (
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE geom IS NOT NULL
                    AND ST_3DDistance(
                            geom,
                            ST_SetSRID(ST_MakePoint(:x2,:y2,:z2), ST_SRID(geom))
                        ) <= :rout
                )
                SELECT a.study_id
                FROM a LEFT JOIN b USING (study_id)
                WHERE b.study_id IS NULL
                ORDER BY a.study_id
                LIMIT 2000
            """)

            ids = conn.execute(q, {
                "x1": c1["x"], "y1": c1["y"], "z1": c1["z"], "rin": r_in,
                "x2": c2["x"], "y2": c2["y"], "z2": c2["z"], "rout": r_out
            }).scalars().all()

        # Concise, human-friendly response
        return jsonify({
            "ok": True,
            "mode": "locations_geom_distance",
            "params": {"coord_a": c1, "coord_b": c2, "r_in": r_in, "r_out": r_out},
            "summary": (
                f'A within {r_in} of ({c1["x"]},{c1["y"]},{c1["z"]}) '
                f'but NOT within {r_out} of ({c2["x"]},{c2["y"]},{c2["z"]})'
            ),
            "a_but_not_b": {
                "count": len(ids),
                "study_ids": ids
            }
        })

    @app.get("/test_db", endpoint="test_db")
    
    def test_db():
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                # Ensure we are in the correct schema
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()

                # Counts
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                # Samples
                try:
                    rows = conn.execute(text(
                        "SELECT study_id, ST_X(geom) AS x, ST_Y(geom) AS y, ST_Z(geom) AS z FROM ns.coordinates LIMIT 3"
                    )).mappings().all()
                    payload["coordinates_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["coordinates_sample"] = []

                try:
                    # Select a few columns if they exist; otherwise select a generic subset
                    rows = conn.execute(text("SELECT * FROM ns.metadata LIMIT 3")).mappings().all()
                    payload["metadata_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["metadata_sample"] = []

                try:
                    rows = conn.execute(text(
                        "SELECT study_id, contrast_id, term, weight FROM ns.annotations_terms LIMIT 3"
                    )).mappings().all()
                    payload["annotations_terms_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["annotations_terms_sample"] = []

            payload["ok"] = True
            return jsonify(payload), 200

        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    return app

# WSGI entry point (no __main__)
app = create_app()
