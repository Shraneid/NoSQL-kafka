# NoSQL-kafka :cactus:

## Présentation de l'API :racehorse:

L’API que nous avons choisi d’utiliser est Blockcypher, qui propose plusieurs endpoints afin de récupérer des informations diverses sur plusieurs blockchains. 
Pour ce projet, nous utilisons deux endpoints en particulier :
-	https://api.blockcypher.com/v1/eth/main
Cet endpoint nous permet de récupérer des données sur la position actuelle de la blockchain. Notamment la hauteur du dernier block miné, le nombre de peers et des statistiques sur le gas. Nous allons nous en servir afin de stocker des données dans un topic compacté, afin de pouvoir accéder aux informations à jour de la tête de la blockchain grâce à un premier consumer. Cela nous permettra aussi de savoir à quelle hauteur de chaine nous sommes, et d’insérer dans un second temps tous les blocks qui ont été minés depuis la dernière fois que nous avons hit l’endpoint dans notre 2ème topic grâce à l’endpoint suivant.

-	https://api.blockcypher.com/v1/eth/main/blocks/$BLOCK_NUMBER 
Cet endpoint-ci, va nous permettre d’autre part, de récupérer les données associées à chaque bloc en spécifiant la hauteur du block que nous voulons.

## Présentation de nos cas d'usage :clipboard:
Pour ce projet, nous avons défini 4 cas d’usage :
1.	Récupérer les « fees » moyens des blocs que nous observons, et convertir ce montant en ETH.
2.	Afficher le nombre moyen de transactions validées dans chaque bloc.
3.	Afficher la hauteur de la blockchain à un instant T.
4.	Récupérer le hash du dernier block miné dans la blockchain.

Notre output :

![usecase1](https://github.com/Shraneid/NoSQL-kafka/blob/main/rapport/casusage1.png)
![usecase2](https://github.com/Shraneid/NoSQL-kafka/blob/main/rapport/casusage2.png)

## Overview du projet :runner::dash:

Nous avons utilisé Kafka pour stocker nos données, couplé a une serialization avec Avro. Nous avons créé deux schéma : main.avsc et blocks.avsc

Exemple avec blocks.avsc :
```json
{
  "name": "BlockClass",
  "type": "record",
  "namespace": "com.gboissinot.esilv.streaming.data.velib",
  "fields": [
    {
      "name": "hash",
      "type": "string"
    },
    {
      "name": "height",
      "type": "int"
    },
    {
      "name": "chain",
      "type": "string"
    },
    {
      "name": "total",
      "type": "double"
    },
    {
      "name": "fees",
      "type": "long"
    },
    {
      "name": "size",
      "type": "int"
    },
    {
      "name": "ver",
      "type": "float"
    },
    {
      "name": "time",
      "type": "string"
    },
    {
      "name": "received_time",
      "type": "string"
    },
    {
      "name": "coinbase_addr",
      "type": "string"
    },
    {
      "name": "relayed_by",
      "type": "string"
    },
    {
      "name": "nonce",
      "type": "double"
    },
    {
      "name": "n_tx",
      "type": "int"
    },
    {
      "name": "prev_block",
      "type": "string"
    },
    {
      "name": "mrkl_root",
      "type": "string"
    },
    {
      "name": "txids",
      "type": {
        "type": "array",
        "items": "string"
      }
    },
    {
      "name": "internal_txids",
      "type": {
        "type": "array",
        "items": "string"
      }
    },
    {
      "name": "depth",
      "type": "int"
    },
    {
      "name": "prev_block_url",
      "type": "string"
    },
    {
      "name": "tx_url",
      "type": "string"
    },
    {
      "name": "next_txids",
      "type": "string"
    }
  ]
}
```

A intervalle régulier, nous récupérons le résultat de l'API main, et nous stockons les valeurs actuelles de la blockchain. Ensuite, nous récupérons tous les blocks qui ont été minés depuis la dernière fois que nous avons hit le main endpoint.

Pour stocker les données, nous avons deux topics : 
- compact, ce topic nous permet de stocker les valeurs actuelles de la blockchain, pour avoir une vue actualisée de la tête de la blockchain Ethereum
- blockcypher, ce topic nous permet de stocker tous les blocks que nous récupérons via l'endpoint block

Pour notre publisher, nous avons créé une méthode générique, prenant un Object en paramètre :
```java
void publish(String tn, String key, Object o) {
    logger.info(String.format("Publishing %s", o));
    ProducerRecord<String, Object> record = new ProducerRecord<>(tn, key, o);
    producer.send(record, (recordMetadata, e) -> {
        if (e != null) {
            logger.error(e.getMessage(), e);
        } else {
            logger.info("Batch record sent.");
        }
    });
}
```

Pour finir, nous avons deux consumers, qui vont récupérer chacun des deux topics, et effectuer les calculs nécessaires pour nos cas d'usage.

- Un premier consumer pour nos données sur les blocks :
```java
double totalFees;
long totalNtxs;
totalFees = 0;
totalNtxs = 0;

blocksConsumer.seekToBeginning(blocksConsumer.assignment());
records = blocksConsumer.poll(Duration.ofMillis(1000));

for (ConsumerRecord<String, BlockClass> record : records) {
  System.out.println(record.value());

  BlockClass o = record.value();

  System.out.println(o.getHeight());

  totalFees += o.getFees();
  totalNtxs += o.getNTx();
}

//sometimes records are empty when starting up the consumer
if (records.count() > 0) {
  System.out.println("Use Case #1 : Get the average fees and convert to ETH");
  System.out.println("avg fees : " + totalFees / records.count() + ", avg fees in ETH : "
          + ((float) (totalFees / records.count()) / 100000000.0f));

  System.out.println("Use Case #2 : Get the average number of transactions");
  System.out.println("avg tx nb : " + totalNtxs / records.count());
}
```

- Un deuxième consumer pour la tête de la blockchain :
```java
mainConsumer.seekToBeginning(mainConsumer.assignment());
mainRecords = mainConsumer.poll(Duration.ofMillis(1000));

for (ConsumerRecord<String, BlockCypher> record : mainRecords) {
    BlockCypher o = record.value();

    if (o.getName().toString().equals("ETH.main")) {
        System.out.println("Use Case #3 : Get the actual height of the chain");
        System.out.println("actual height : " + o.getHeight());

        System.out.println("Use Case #4 : Get the hash of the last mined block");
        System.out.println("hash of last mined block : " + o.getHash());
    }
}
```

## Authors :couple_with_heart: :two_men_holding_hands:
- Quentin Tourette
- Victor Larrezet
