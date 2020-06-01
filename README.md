# HBaseProjet

## Installation

Le projet requis Java 8 ou supérieur ainsi que de Maven

L'installation du projet se fait par les commandes suivantes

```bash
cd HBaseClient
mvn install
```

## Exécution

Il est requis que ZooKeeper, MasterServer et RegionServer tournent lors de l'exécution du projet.

Le projet se connecte par les ports par défaut de ceux-ci (Zookeeper 2181, MasterServer 16000, RegionServer 16010) en localhost.

Ci tout cela est fait, il suffit d'exécuter les commandes suivantes pour lancer le programme

```bash
cd HBaseClient
mvn exec:java
```
