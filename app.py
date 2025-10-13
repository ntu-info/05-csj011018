# app.py
from flask import Flask, jsonify, abort, send_file, request
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
    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_terms(term_a: str, term_b: str):
        """
        在 ns.metadata.title 做子字串比對：
        - 包含 term_a
        - 並且不包含 term_b
        例：
        /dissociate/terms/posterior_cingulate/ventromedial_prefrontal
        """
        # 1) 正規化：底線 → 空白；去頭尾空白
        def norm(s: str) -> str:
            return s.replace("_", " ").strip()

        ta = norm(term_a)
        tb = norm(term_b)

        # 2) LIKE 模式（ILIKE 為大小寫不敏感）
        pat_a = f"%{ta}%"
        pat_b = f"%{tb}%"

        # 可選：?limit= 回傳上限（預設 1000）
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
                    WHERE title ILIKE :pat_a
                      AND title NOT ILIKE :pat_b
                    ORDER BY study_id
                    LIMIT :lim
                """),
                {"pat_a": pat_a, "pat_b": pat_b, "lim": limit},
            ).mappings().all()

        results = [dict(r) for r in rows]

        return jsonify({
            "ok": True,
            "mode": "title_contains_a_not_b",
            "params": {"term_a": term_a, "term_b": term_b},
            "count": len(results),
            "studies": results
        })
    
    """
    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords):
        x, y, z = map(int, coords.split("_"))
        return jsonify([x, y, z])
    """
    # --- Dissociate by LOCATIONS: "A(x1,y1,z1) but not B(x2,y2,z2)" ---
    from flask import request  # 檔頭記得已經有

    @app.get("/dissociate/locations/<coords1>/<coords2>", endpoint="dissociate_locations")
    def dissociate_locations(coords1: str, coords2: str):
        """
        A: 距離 coords1 在 r_in 以內
        B: 距離 coords2 在 r_out 以內
        回傳 A\B（靠近 coords1 且 不靠近 coords2）
        參數：
            - coords1/coords2: "x_y_z"（可浮點）
            - ?r_in=2  、?r_out=2  （單位同座標，MNI 通常 mm）
        """
        def parse_xyz(s: str):
            x, y, z = [float(tok) for tok in s.split("_")]
            return {"x": x, "y": y, "z": z}

        c1 = parse_xyz(coords1)
        c2 = parse_xyz(coords2)

        try:
            r_in  = float(request.args.get("r_in", "2"))
            r_out = float(request.args.get("r_out", "2"))
        except Exception:
            r_in, r_out = 2.0, 2.0

        r_in2  = r_in  * r_in
        r_out2 = r_out * r_out

        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            # 用乘法做距離平方，避免使用 ^（在 PG 不是次方）
            q = text("""
                WITH a AS (
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
                        AND ((x - :x1)*(x - :x1) + (y - :y1)*(y - :y1) + (z - :z1)*(z - :z1)) <= :rin2
                ),
                b AS (
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
                        AND ((x - :x2)*(x - :x2) + (y - :y2)*(y - :y2) + (z - :z2)*(z - :z2)) <= :rout2
                )
                SELECT a.study_id
                FROM a
                LEFT JOIN b USING (study_id)
                WHERE b.study_id IS NULL
                ORDER BY a.study_id
                LIMIT 2000
            """)
            ids = conn.execute(q, {
                "x1": c1["x"], "y1": c1["y"], "z1": c1["z"], "rin2": r_in2,
                "x2": c2["x"], "y2": c2["y"], "z2": c2["z"], "rout2": r_out2
            }).scalars().all()

        return jsonify({
            "ok": True,
            "mode": "locations_xyzdist",
            "params": {"coord_a": c1, "coord_b": c2, "r_in": r_in, "r_out": r_out},
            "a_but_not_b": {"count": len(ids), "study_ids": ids[:200]}
        })

    @app.get("/dissociate/locations/<coords1>/<coords2>", endpoint="dissociate_locations")
    def dissociate_locations(coords1: str, coords2: str):
        """
        用 coordinates.parquet 的 x,y,z 欄位做 3D 歐式距離：
        - A：距離 coords1 在 r_in 以內（含邊界）
        - B：距離 coords2 在 r_out 以內（含邊界）
        回傳 A \ B，也就是「靠近 coords1 且 不靠近 coords2」的 study_id。
        
        參數：
        - coords1 / coords2：格式 "x_y_z"（底線分隔），支援整數或小數
        - ?r_in=2  ：coords1 的內圈半徑（預設 2，單位同座標單位，MNI 通常 mm）
        - ?r_out=2 ：coords2 的外圈半徑（預設 2）
        """
        def parse_xyz(s: str):
            try:
                x, y, z = [float(tok) for tok in s.split("_")]
            except Exception:
                abort(400, f"Invalid coordinate format: {s}. Use x_y_z, e.g., 0_-52_26")
            return {"x": x, "y": y, "z": z}

        c1 = parse_xyz(coords1)
        c2 = parse_xyz(coords2)

        # 半徑參數（可用 querystring 覆蓋）
        try:
            r_in = float(request.args.get("r_in", "2"))
        except Exception:
            r_in = 2.0
        try:
            r_out = float(request.args.get("r_out", "2"))
        except Exception:
            r_out = 2.0

        r_in2  = r_in  * r_in   # 距離平方，避免在 SQL 裡開根號
        r_out2 = r_out * r_out

        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            # 用 x,y,z 欄位直接做歐式距離平方（NULL 先排除）
            q = text("""
                WITH a AS (
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
                      AND ((x - :x1)*(x - :x1) + (y - :y1)*(y - :y1) + (z - :z1)*(z - :z1)) <= :rin2
                ),
                b AS (
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
                      AND ((x - :x2)*(x - :x2) + (y - :y2)*(y - :y2) + (z - :z2)*(z - :z2)) <= :rout2
                )
                SELECT a.study_id
                FROM a
                LEFT JOIN b USING (study_id)
                WHERE b.study_id IS NULL
                ORDER BY a.study_id
                LIMIT 2000
            """)
            ids = conn.execute(q, {
                "x1": c1["x"], "y1": c1["y"], "z1": c1["z"], "rin2": r_in2,
                "x2": c2["x"], "y2": c2["y"], "z2": c2["z"], "rout2": r_out2
            }).scalars().all()

            # 帶些 metadata 方便檢視
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
            "mode": "locations_xyzdist",
            "params": {
                "coord_a": c1, "coord_b": c2,
                "r_in": r_in, "r_out": r_out
            },
            "a_but_not_b": {
                "count": len(ids),
                "study_ids": ids[:200],
                "sample_metadata": meta
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
