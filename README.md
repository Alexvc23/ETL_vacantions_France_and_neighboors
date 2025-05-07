Documentation du Pipeline ETL Vacances Scolaires
Ce document décrit le pipeline ETL (Extract, Transform, Load) conçu pour traiter les données de vacances scolaires à partir de fichiers CSV et les charger dans une base de données PostgreSQL.

1. Objectif
L'objectif principal de ce pipeline est de consolider les données de vacances scolaires pour la France et les pays voisins (Belgique, Allemagne, Suisse, Italie, Espagne, Luxembourg) issues de fichiers CSV, de les transformer en un format binaire journalier (indiquant la présence ou l'absence de vacances pour chaque région), et de charger le résultat dans une table de base de données (t_vacances_scolaires) pour une analyse ultérieure.

2. Composants du Pipeline
Le pipeline est constitué des fichiers suivants :

config.json: Fichier de configuration JSON spécifiant les chemins des fichiers CSV source.

t_vacances_etl.py: Module Python contenant la logique de l'ETL (extraction, transformation, chargement).

main.py: Script principal Python pour exécuter le pipeline ETL, gérant les arguments en ligne de commande et la lecture de la configuration.

etl_pipeline_test.ipynb: Notebook Jupyter pour tester et valider les différentes étapes du pipeline.

2.1. config.json
Ce fichier au format JSON est utilisé pour spécifier les chemins des fichiers CSV contenant les données brutes des vacances.

{
    "csv_files": [
        "/chemin/vers/fr-en-calendrier-scolaire.csv",
        "/chemin/vers/fr-en-calendrier-scolaire-remaining.csv"
    ]
}


Le pipeline lit la liste des chemins de fichiers CSV à partir de la clé "csv_files".

2.2. t_vacances_etl.py
Ce module contient les fonctions principales pour chaque étape de l'ETL :

get_engine(): Établit la connexion à la base de données PostgreSQL en utilisant les variables d'environnement (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS). Les informations d'identification par défaut sont configurées pour Khubeo_IA.

load_csvs(csv_paths): Charge un ou plusieurs fichiers CSV dans un DataFrame pandas. Les fichiers sont supposés être délimités par des points-virgules (;) et encodés en UTF-8 avec BOM.

_extract_region_codes(row): Fonction utilitaire (interne) pour extraire les codes de région (vac_fr_zone_a, vac_bel, etc.) à partir d'une ligne du DataFrame brut, en se basant sur les colonnes 'Zones' et 'Académies'. Gère le cas spécifique de la Corse.

transform_to_binary(df): Transforme le DataFrame brut en un format binaire journalier. Pour chaque jour, il crée une colonne pour chaque région/zone et indique par 1 si c'est un jour de vacances dans cette région, et 0 sinon. Il gère l'expansion des plages de dates et l'agrégation par date.

load_final_table(engine, df, table_name="t_vacances_scolaires"): Charge le DataFrame transformé dans la table spécifiée dans la base de données. La table existante est remplacée (if_exists="replace"). Les types de données SQL appropriés (Date, Integer) sont appliqués.

run_etl(csv_paths, *, sql_dir=None): Orchestre l'exécution du pipeline ETL. Il appelle successivement les fonctions de chargement, transformation et chargement final. Il permet également l'exécution optionnelle de scripts SQL supplémentaires après le chargement de la table finale.

2.3. main.py
Le script main.py est le point d'entrée pour l'exécution du pipeline. Il utilise argparse pour gérer les arguments en ligne de commande et glob pour l'expansion des motifs de fichiers.

Il peut prendre en entrée un ou plusieurs chemins de fichiers CSV directement en argument.

Il peut également utiliser l'argument --json-config pour spécifier un fichier JSON (par défaut config.json) contenant la liste des chemins CSV. Si aucun chemin CSV n'est fourni directement, il essaie de charger les chemins depuis le fichier JSON par défaut ou spécifié.

Il change le répertoire de travail (os.chdir) pour s'assurer que les modules nécessaires sont correctement importés.

Il appelle la fonction run_etl du module t_vacances_etl.py avec les chemins des fichiers CSV collectés.

2.4. etl_pipeline_test.ipynb
Ce notebook Jupyter fournit un environnement interactif pour tester et valider les différentes étapes du pipeline ETL. Il contient des cellules pour :

Configurer les variables d'environnement pour la connexion à la base de données.

Vérifier la connexion à la base de données.

Charger un fichier CSV source et afficher les premières lignes.

Exécuter le pipeline ETL complet en appelant run_etl.

Lire la table finale t_vacances_scolaires depuis la base de données dans un DataFrame pandas.

Afficher les colonnes et les premières lignes de la table finale.

Effectuer des vérifications spécifiques, comme l'analyse des vacances d'été ou d'hiver pour une année donnée, afin de valider l'exactitude des données transformées.

3. Structure de la Base de Données
Le pipeline crée ou remplace une table nommée t_vacances_scolaires. La structure de cette table est la suivante :

vac_date (Date) : La date du jour.

vac_fr_zone_a (Integer) : Indicateur binaire (1 si vacances, 0 sinon) pour la Zone A en France.

vac_fr_zone_b (Integer) : Indicateur binaire pour la Zone B en France.

vac_fr_zone_c (Integer) : Indicateur binaire pour la Zone C en France.

vac_fr_corse (Integer) : Indicateur binaire pour la Corse.

vac_all (Integer) : Indicateur binaire pour l'Allemagne (zone de_by).

vac_bel (Integer) : Indicateur binaire pour la Belgique (zone be_nl).

vac_esp (Integer) : Indicateur binaire pour l'Espagne (zone es_ga).

vac_ita (Integer) : Indicateur binaire pour l'Italie (zone it_bz).

vac_lux (Integer) : Indicateur binaire pour le Luxembourg (pays entier).

vac_sui (Integer) : Indicateur binaire pour la Suisse (zone ch_zh).

vac_annee_scolaire (Text) : L'année scolaire correspondante.

4. Utilisation
Pour exécuter le pipeline, assurez-vous que les variables d'environnement pour la connexion à la base de données sont correctement configurées.

Vous pouvez exécuter le script main.py de différentes manières :

En spécifiant les fichiers CSV directement :

python main.py /chemin/vers/fr-en-calendrier-scolaire.csv /chemin/vers/fr-en-calendrier-scolaire-remaining.csv


En utilisant un motif glob :

python main.py "/chemin/vers/*.csv"


En utilisant le fichier de configuration JSON (par défaut config.json) :

python main.py


En spécifiant un autre fichier de configuration JSON :

python main.py --json-config /chemin/vers/mon_config.json


Le script affichera des messages indiquant les étapes de l'exécution et le nombre de lignes chargées dans la table finale.

5. Développement et Tests
Le notebook etl_pipeline_test.ipynb est un outil précieux pour le développement, le débogage et la validation du pipeline. Il permet d'exécuter chaque étape individuellement et d'inspecter les données à chaque phase.

6. Remarques
Les chemins des fichiers CSV dans config.json et en arguments de main.py doivent être accessibles depuis l'environnement où le script est exécuté.

Le script main.py change le répertoire de travail, ce qui peut nécessiter des ajustements si la structure du projet est modifiée.

