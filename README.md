# KTE_artesian

Connettore ad Artesian per python sviluppato da Key To Energy

## Artesian 
Questa sezione contene le funzioni che incartano il pacchetto sdk-artesian per python rendendo 
l'interazione con
il sistema d'archiviazione Artesian piu rapide e semplici.
Artesian è uno strumento per archiviare time series, ovvero dei valori discreti legati ad indice 
temporale di granularità variabile, ogni tipologia di curva presenta il suo modulo contenente 
tutte le funzioni utili ad interagirci.
</br>
### Versioned Time Series

Le Versioned Time Series servono per memorizzare curve discrete, la cui ordinata è il tempo che presentano
la necessità di essere vesionate ad ogni caricamento.</br>
Le funzioni sono situate nel sotto modulo KTE_artesian_versioned.py, e possono essere importate 
tramite il comando **inserire il comando**

#### Funzioni di utilità generale
<dl>
  <dt>get_version(query, time_version, version_info1='P0Y1M0D', version_info2=False)</dt>
  <dd>Rende la versione specificata tramite i parametri della funzione</br>
        :param query: Oggetto della classe QueryService contenuto nell pacchetto sdk-artesian</br>
        :param time_version: Stringa contenente il nome della metodologia d'estrazione di versione richiesta</br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;  muv => Ultima versione inserita</br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;  lastNVersion => Ultime n versioni</br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;  version => Versione specifica, bisogna passare la stringa contenente il time stamp</br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;  lastOfDays => Rende l'ultima versione del giorno specificato come stringa</br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;  lastOfMonth => Rende l'ultima versione del mese specificato come Stringa</br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;  mostRecent => Rende la versione più recente contenuta nell'intervallo temporale rappresentato</br>
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                dalle due stringhe passate come input</br>
        :param version_info1: Stringa contenente una data o un intero rappresentante il numero di versioni richieste</br>
        :param version_info2: Stringa contenente una data rappresentante il secondo estremo di un intervallo temporale</br>
        :return: dataset contenente la curva nella versione richiesta.</dd>
  <dt>Second Term</dt>
  <dd>This is one definition of the second term. </dd>
  <dd>This is another definition of the second term.</dd>
</dl>