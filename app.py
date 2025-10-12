# app.py
from flask import Flask, jsonify, abort, send_file
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

    @app.get("/terms/<term>/studies", endpoint="terms_studies")
    def get_studies_by_term(term):
        return term

    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords):
        x, y, z = map(int, coords.split("_"))
        return jsonify([x, y, z])

    @app.get("/test_db", endpoint="test_db")

    # --- Dissociate by TERMS: "A but not B" ---
    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_terms(term_a: str, term_b: str):
        """
        Return studies that mention term_a BUT NOT term_b.
        Example:
        /dissociate/terms/posterior_cingulate/ventromedial_prefrontal
        """
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))

            q = text("""
                WITH a AS (
                    SELECT DISTINCT study_id
                    FROM ns.annotations_terms
                    WHERE term = :term_a
                ),
                b AS (
                    SELECT DISTINCT study_id
                    FROM ns.annotations_terms
                    WHERE term = :term_b
                )
                SELECT a.study_id
                FROM a
                LEFT JOIN b USING (study_id)
                WHERE b.study_id IS NULL
                ORDER BY a.study_id
                LIMIT 1000
            """)
            rows = conn.execute(q, {"term_a": term_a, "term_b": term_b}).scalars().all()

            return jsonify({
                "ok": True,
                "mode": "terms",
                "a_but_not_b": {
                    "term_a": term_a,
                    "term_b": term_b,
                    "study_ids": rows,
                    "count": len(rows)
                }
            })

    # --- Dissociate by LOCATIONS: "A(x1,y1,z1) but not B(x2,y2,z2)" ---
    @app.get("/dissociate/locations/<coords1>/<coords2>", endpoint="dissociate_locations")
    def dissociate_locations(coords1: str, coords2: str):
        """
        Coordinates are passed as x_y_z (underscores, not commas).
        Example:
        /dissociate/locations/0_-52_26/-2_50_-6
        """
        def parse_xyz(s: str):
            x, y, z = map(int, s.split("_"))
            return {"x": x, "y": y, "z": z}

        c1 = parse_xyz(coords1)
        c2 = parse_xyz(coords2)

        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))

            q = text("""
                WITH a AS (
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE x = :x1 AND y = :y1 AND z = :z1
                ),
                b AS (
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE x = :x2 AND y = :y2 AND z = :z2
                )
                SELECT a.study_id
                FROM a
                LEFT JOIN b USING (study_id)
                WHERE b.study_id IS NULL
                ORDER BY a.study_id
                LIMIT 1000
            """)
            rows = conn.execute(q, {
                "x1": c1["x"], "y1": c1["y"], "z1": c1["z"],
                "x2": c2["x"], "y2": c2["y"], "z2": c2["z"],
            }).scalars().all()

            return jsonify({
                "ok": True,
                "mode": "locations",
                "a_but_not_b": {
                    "coord_a": c1,
                    "coord_b": c2,
                    "study_ids": rows,
                    "count": len(rows)
                }
            })

    
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
