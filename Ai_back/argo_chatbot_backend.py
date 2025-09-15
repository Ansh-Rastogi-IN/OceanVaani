import sqlite3
import re
import logging
import pandas as pd
import numpy as np
from place_coords import PLACE_COORDS

logger = logging.getLogger(__name__)

try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False


class ArgoDataChatbot:
    def __init__(self, llm=None, db_path="synthetic_argo_1M_upsampled_cleaned.db",
                 faiss_index_path="synthetic_argo_vectors_final.index"):
        self.llm = llm
        self.db_path = db_path
        self.default_k = 10

        # Load FAISS index if available
        self.faiss_index = None
        if FAISS_AVAILABLE:
            try:
                self.faiss_index = faiss.read_index(faiss_index_path)
                logger.info("FAISS index loaded successfully.")
            except Exception as e:
                logger.warning(f"Could not load FAISS index: {e}")

    # ---------- Query parsing (LLM + fallback) ----------
    def parse_query(self, query: str) -> str:
        """
        Convert natural language query to SQL.
        """
        # Removed LLM logic and always use fallback to avoid 'chain' not defined error
        return self._fallback_parse_query(query)

    def _fallback_parse_query(self, query: str) -> str:
        """
        Build a SQL query programmatically:
         - Uses place -> lat/lon via PLACE_COORDS
         - Uses FAISS to find nearest rows and returns SELECT ... WHERE id IN (...)
         - If FAISS is not available, builds a simple WHERE clause
        """
        q_lower = query.lower()

        # parameters in DB
        parameters = ['temperature', 'salinity', 'pressure',
                      'dissolved_oxygen', 'dissolved oxygen', 'oxygen']

        # normalize param
        param = next((p for p in parameters if p.replace(' ', '_') in q_lower or p in q_lower), None)
        if param:
            param_norm = param.replace(' ', '_')
            if param_norm == 'oxygen':
                param_norm = 'dissolved_oxygen'
        else:
            param_norm = 'temperature'

        # Extract year
        year_match = re.search(r'\b(20\d{2})\b', query)
        year = int(year_match.group(1)) if year_match else None

        # Match place
        place = None
        for pname in PLACE_COORDS.keys():
            if pname.lower().replace('_', ' ') in q_lower or pname.lower() in q_lower:
                place = pname
                break

        # If FAISS available, use nearest-neighbor retrieval
        if place and FAISS_AVAILABLE and self.faiss_index is not None:
            ids = self.query_nearest_by_place(place, year=year,
                                              parameter=param_norm, k=self.default_k)
            if ids:
                id_list = ",".join(str(int(i)) for i in ids)
                sql = f"""
                    SELECT id, date, latitude, longitude, {param_norm}
                    FROM synthetic_argo_final
                    WHERE id IN ({id_list});
                """
                return sql

        # fallback query
        sql = f"""
            SELECT id, date, latitude, longitude, {param_norm}
            FROM synthetic_argo_final
            LIMIT 50;
        """
        return sql

    # ---------- DB Execution ----------
    def execute_sql(self, sql: str):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df

    # ---------- FAISS helper ----------
    def query_nearest_by_place(self, place, year=None, parameter="temperature", k=10):
        # map place -> lat/lon
        lat, lon = PLACE_COORDS.get(place, (0.0, 0.0))
        vector = [lat, lon]
        vector = np.array(vector).astype('float32').reshape(1, -1)

        if self.faiss_index is None:
            return []

        _, ids = self.faiss_index.search(vector, k)
        return ids.flatten().tolist()