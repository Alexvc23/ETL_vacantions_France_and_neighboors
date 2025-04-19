/* ===============================================================
   0)  Nettoyage : on supprime la table précédente si elle existe
   =============================================================== */
DROP TABLE IF EXISTS region_vacances;

/* ===============================================================
   1)  Création de la nouvelle table t_region_vacances
   =============================================================== */
CREATE TABLE t_region_vacances (
    code_region  TEXT  PRIMARY KEY,
    libelle      TEXT  NOT NULL,
    pays         TEXT  NOT NULL,
    academies    TEXT,
    remarques    TEXT
);

/* ===============================================================
   2)  Chargement des données (répartition MENJ 2025‑2026)
   =============================================================== */
INSERT INTO t_region_vacances (code_region, libelle, pays, academies, remarques) VALUES
    /* ─── France métropolitaine : zones scolaires ──────────────── */
    ('fr_zone_a',
        'Zone A – France métropolitaine',
        'France',
        'Besançon, Bordeaux, Clermont‑Ferrand, Dijon, Grenoble, Limoges, Lyon, Poitiers',
        'Répartition officielle MENJ 2024‑2026'),
    ('fr_zone_b',
        'Zone B – France métropolitaine',
        'France',
        'Aix‑Marseille, Amiens, Lille, Nancy‑Metz, Nantes, Nice, Normandie, Orléans‑Tours, Reims, Rennes, Strasbourg',
        'Répartition officielle MENJ 2024‑2026 ; Académie de Normandie = ex‑Caen + Rouen'),
    ('fr_zone_c',
        'Zone C – France métropolitaine',
        'France',
        'Créteil, Montpellier, Paris, Toulouse, Versailles',
        'Répartition officielle MENJ 2024‑2026'),

    /* ─── Collectivité à calendrier spécifique ─────────────────── */
    ('fr_zone_corse',
        'Corse',
        'France',
        NULL,
        'Académie de Corse ; calendrier distinct'),

    /* ─── Pays frontaliers / voisins (inchangés) ───────────────── */
    ('allemagne',   'Allemagne',   'Allemagne',    NULL, NULL),
    ('belgique',    'Belgique',    'Belgique',     NULL, NULL),
    ('espagne',     'Espagne',     'Espagne',      NULL, NULL),
    ('italie',      'Italie',      'Italie',       NULL, NULL),
    ('suisse',      'Suisse',      'Suisse',       NULL, NULL),
    ('andorre',     'Andorre',     'Andorre',      NULL, NULL),
    ('monaco',      'Monaco',      'Monaco',       NULL, NULL),
    ('luxembourg',  'Luxembourg',  'Luxembourg',   NULL, NULL);
