# Example of Apache Beam pipeline which reads data from PubSub topic and write into Firebase
just simple example/proof of concept

## Setup
in file `df_firestore_stream.py` set GCP project and BUCKET (if you want to run on Dataflow)  
`run_local` variable in the file controls whether pipeline works locally or on Dataflow  
to run pipeline just execute `python df_firestore_stream.py`  

`publish.py` just publishes messages to PubSub, for testing purposes. in variable `PROJECT` set GCP project

At the moment this doesn't work with Apache Beam 2.13.0 but works with 2.12.0. Python 2.7 was used
 

