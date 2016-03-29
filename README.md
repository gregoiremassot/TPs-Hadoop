# TPs Hadoop

Voici les TPs réalisés lors du cours Big Data en 3ème année à l'École des Mines, entre Septembre et Decembre 2015.

## tiny_graph.txt

Le fichier d'entrée *tiny_graph.txt* est un graphe de pages web. Chaque ligne correspond à une page web. Pour chaque ligne *i* :

* Le premier nombre est le numéro associé à la page web *i*
* Le second nombre est le PageRank initial de la page *i*
* Les nombres suivants sont les numéros associés aux pages qui figurent dans les liens hypertextes de la page *i*

## pageRank.java

C'est le programme qui va calculer les Page Rank des pages contenues dans *tiny_graph.txt*. On a les

## script.sh

*script.sh* permet d'automatiser l'exécution du programme sur le cluster Hadoop

## part-r-000000

C'est le fichier de sortie fourni par notre cluster Hadoop. Chaque ligne représente une page web. Pour chaque ligne *i*

* Le premier nombre correspond au numéro de la page web *i*
* Le second nombre est le PageRank de la page après calcul.