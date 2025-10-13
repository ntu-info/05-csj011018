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
    """
    @app.get("/terms/<term>/studies", endpoint="terms_studies")
    def get_studies_by_term(term):
        return term
    """
    # --- Dissociate by TERMS: "A but not B" ---
    from flask import Flask, request
    @app.get("/terms/<term>/studies", endpoint="terms_studies")
    def get_studies_by_term(term: str):
        """
        回傳：有提到 term 的 study_id（可用 ?minw=0.01 設定 TF-IDF 門檻）
        例：/terms/posterior_cingulate/studies?minw=0.01
        """
        def norm(s: str) -> str:
            return s.replace("_", " ").strip().lower()

        try:
            minw = float(request.args.get("minw", "0"))
        except Exception:
            minw = 0.0

        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            q = text("""
                SELECT DISTINCT study_id
                FROM ns.annotations_terms
                WHERE lower(term) = :term AND weight > :minw
                ORDER BY study_id
                LIMIT 2000
            """)
            ids = conn.execute(q, {"term": norm(term), "minw": minw}).scalars().all()

            meta = []
            if ids:
                rows = conn.execute(text("""
                    SELECT m.study_id, m.title, m.journal, m.year
                    FROM ns.metadata m
                    WHERE m.study_id = ANY(:ids)
                    ORDER BY m.study_id
                    LIMIT 100
                """), {"ids": ids}).mappings().all()
                meta = [dict(r) for r in rows]

        return jsonify({
            "ok": True,
            "mode": "term_only",
            "term": term,
            "min_weight": minw,
            "count": len(ids),
            "study_ids": ids[:200],
            "sample_metadata": meta
        })
    """
    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords):
        x, y, z = map(int, coords.split("_"))
        return jsonify([x, y, z])
    """
    # --- Dissociate by LOCATIONS: "A(x1,y1,z1) but not B(x2,y2,z2)" ---
    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords: str):
        """
        回傳：在 coords 附近（3D 半徑）出現過座標的 studies
        - 座標格式：x_y_z，例如 0_-52_26
        - 半徑（mm）：?radius=2  預設 2
        """
        def parse_xyz(s: str):
            x, y, z = [float(tok) for tok in s.split("_")]
            return {"x": x, "y": y, "z": z}

        try:
            radius = float(request.args.get("radius", "2"))
        except Exception:
            radius = 2.0

        c = parse_xyz(coords)

        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            q = text("""
                SELECT DISTINCT study_id
                FROM ns.coordinates
                WHERE ST_3DDistance(geom, ST_MakePoint(:x,:y,:z)) <= :r
                ORDER BY study_id
                LIMIT 2000
            """)
            ids = conn.execute(q, {"x": c["x"], "y": c["y"], "z": c["z"], "r": radius}).scalars().all()

            meta = []
            if ids:
                rows = conn.execute(text("""
                    SELECT m.study_id, m.title, m.journal, m.year
                    FROM ns.metadata m
                    WHERE m.study_id = ANY(:ids)
                    ORDER BY m.study_id
                    LIMIT 100
                """), {"ids": ids}).mappings().all()
                meta = [dict(r) for r in rows]

        return jsonify({
            "ok": True,
            "mode": "location_only",
            "coord": c,
            "radius_mm": radius,
            "count": len(ids),
            "study_ids": ids[:200],
            "sample_metadata": meta
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
