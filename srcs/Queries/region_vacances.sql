/* ================================================================
   TABLE DE RÉFÉRENCE DES RÉGIONS / ZONES DE VACANCES SCOLAIRES
   Inspirée du fichier CSV officiel “calendrier‑scolaire‑toutes‑académies”
   ================================================================ */
CREATE TABLE region_vacances (
    code_region   TEXT        PRIMARY KEY,              -- clé logique utilisée dans vos données
    libelle       TEXT        NOT NULL,                 -- nom lisible
    pays          TEXT        NOT NULL,                 -- pays ou collectivité
    academies     TEXT                                    -- liste des académies (pour les zones A/B/C)
                       DEFAULT NULL,
    remarques     TEXT                                    -- toute information utile
                       DEFAULT NULL
);

/* ----------------------------------------------------------------
   DONNÉES D’AMORÇAGE  (tout le libellé et les commentaires en FR)
   ---------------------------------------------------------------- */
INSERT INTO region_vacances (code_region, libelle, pays, academies, remarques) VALUES
    /*  ─── France métropolitaine : zones scolaires ───────────────── */
    ('fr_zone_a',
        'Zone A – France métropolitaine',
        'France',
        'Besançon, Bordeaux, Clermont‑Ferrand, Dijon, Grenoble, Limoges, Lyon, Poitiers',
        'Répartition officielle MENJ (2024‑2025)'),
    ('fr_zone_b',
        'Zone B – France métropolitaine',
        'France',
        'Aix‑Marseille, Amiens, Caen, Lille, Nancy‑Metz, Nantes, Nice, Orléans‑Tours, Reims, Rennes, Rouen, Strasbourg',
        'Répartition officielle MENJ (2024‑2025)'),
    ('fr_zone_c',
        'Zone C – France métropolitaine',
        'France',
        'Créteil, Montpellier, Paris, Toulouse, Versailles',
        'Répartition officielle MENJ (2024‑2025)'),

    /*  ─── Collectivités françaises disposant de leur propre calendrier ─── */
    ('fr_zone_corse',
        'Zone Corse',
        'France',
        NULL,
        'Académie de Corse ; suit un calendrier spécifique'),

    /*  ─── Pays frontaliers / limitrophes ─────────────────────────── */
    ('allemagne',   'Allemagne',    'Allemagne',     NULL, NULL),
    ('belgique',    'Belgique',     'Belgique',      NULL, NULL),
    ('espagne',     'Espagne',      'Espagne',       NULL, NULL),
    ('italie',      'Italie',       'Italie',        NULL, NULL),
    ('suisse',      'Suisse',       'Suisse',        NULL, NULL),
    ('andorre',     'Andorre',      'Andorre',       NULL, NULL),
    ('monaco',      'Monaco',       'Monaco',        NULL, NULL),
    ('luxembourg',  'Luxembourg',   'Luxembourg',    NULL, NULL);

/*  Remarque : 
    - Vous pouvez ajouter d’autres territoires ultramarins (Guadeloupe, Guyane, etc.) 
      en vous appuyant sur les colonnes “Location” et “Description” du CSV.
    - Les listes d’académies ont été extraites automatiquement du fichier CSV fourni
      (période 2018‑2025).  Mettez‑les à jour si la carte scolaire change.           */
