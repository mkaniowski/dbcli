1. ``` SELECT expand(out()) FROM V WHERE name = "Planned_cities_by_country" ```
2. ``` SELECT out().size() FROM V WHERE name = "Planned_cities_by_country" ```
3. ``` SELECT expand(out()).out() FROM V WHERE name = "Planned_cities_by_country" ```
4. ``` SELECT expand(in()) FROM V WHERE name = "Planned_cities_by_country" ```
5. ``` SELECT in().size() FROM V WHERE name = "Planned_cities_by_country" ```
6. ``` SELECT expand(in().in()) FROM V WHERE name = "Planned_cities_by_country" ```
7. ``` SELECT count(distinct(name)) FROM V ```
8. ``` SELECT * FROM V WHERE in().size() = 0 ```
9. ``` SELECT count(*) FROM V WHERE in().size() = 0 ```
10. ``` SELECT FROM V WHERE out().size() = (SELECT max(out().size()) FROM V) ```
11. ``` SELECT FROM V WHERE out().size() = (SELECT min(out().size()) FROM V WHERE out().size() > 0) ```
12. ``` UPDATE V SET name = 'xfafafafa' WHERE name = 'Planned_cities_by_country' ```
13. ``` UPDATE V SET popularity = 13213131 WHERE name = 'xfafafafa' ```
14. ``` 
    SELECT expand(both()) FROM (
    TRAVERSE out() FROM (SELECT FROM V WHERE name = '2005_in_Oceanian_association_football_leagues')
    WHILE $depth <= 6 AND @rid != (SELECT @rid FROM V WHERE name = 'Christianity_in_Bolivia')
    )
    ```

15. ``` SELECT count(*) FROM (
    TRAVERSE out() FROM (SELECT FROM V WHERE name = '2005_in_Oceanian_association_football_leagues')
    WHILE $depth <= 6 AND @rid != (SELECT @rid FROM V WHERE name = 'Christianity_in_Bolivia')
    ) 
    ```

16. ``` SELECT sum(popularity) FROM (
    TRAVERSE both() FROM (SELECT FROM V WHERE name = '2005_in_Oceanian_association_football_leagues')
    WHILE $depth <= 6
    )
    ```
17. ```
    SELECT sum(popularity) FROM
    (
    SELECT expand(path) FROM (
    SELECT (shortestPath((SELECT FROM V WHERE name = '2005_in_Oceanian_association_football_leagues'), (SELECT FROM V WHERE name = 'December_1976_sports_events_in_Europe'))) as path
    ) UNWIND path
    )
    ```
18.