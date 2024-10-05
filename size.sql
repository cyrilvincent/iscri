SELECT table_name,
pg_relation_size(table_schema || '.' || table_name) / 1024 / 1024 As Taille_donnees,
pg_total_relation_size(table_schema || '.' || table_name) / 1024 / 1024 As Taille_totale
FROM information_schema.tables
ORDER BY Taille_totale DESC