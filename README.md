# Hadoop training repository
This repository contains documentation and code used during a Hadoop training course for programming novices. It is geared towards using Google Cloud Platform (GCP) Dataproc.

We assume that you have already created your GCP project.

## Data used for exercises
The data used for the exercises is not provided since it's subject to a NDA. It has been shared on Google Cloud Storage (GCS) with participants for the duration of the training. Participants should copy the data to their own buckets. Note, that the data volume exceeds the 5GB always-free storage limit by GCP. If you don't want to continue spending your USD 300 free-tier credit you may want to delete the data at the end of training. See [GCP pricing](https://cloud.google.com/pricing/) for details.

## Interacting with your GCP project
We will interact with our Google cloud in two ways:
- with `glcoud command line utility
- via the GCP [console](https://console.cloud.google.com) web interface.

Please install `gcloud` by following the instructions in the GCP documentation.

Not all GCP operations are available via the web interface. Also, we will sometimes use `google beta` commands below. These give access to some advanced features. Finally, try using `google alpha shell` to have auto-completion in commands. This can help save a lot of time.

## Create a Dataproc cluster using `gcloud`
Dataproc has Hadoop MapReduce and Spark pre-installed. You can create a Dataproc cluster from the GCP [console](https://console.cloud.google.com) in your browser but also using the `gcloud` utility you have installed on your computer.

The command line below creates a 3 node cluster that shuts down after 30 minutes of inactivity. You need to chose a `<your-cluster-name>`.  Notice the `beta`, which is required to configure `--max-idle`. 
```
gcloud beta dataproc \
    clusters create <your-cluster-name> \
    --max-idle 30m \
    --region europe-west1 \
    --zone europe-west1-d \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 500 \
    --num-workers 2 \
    --worker-machine-type n1-standard-4 \
    --worker-boot-disk-size 500 \
    --image-version 1.2
```

## Google Dataproc cluster user interface
In order to see the cluster's web interfaces in your browser we need to set up a proxy-server and make Chrome use that proxy server. You can find the same instructions in the [documentation](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces).

### Step 1: Set up an SSH tunnel using `gcloud`
Below, replace `<cluster-master-host>` with the name your your master hostname. This will always be `<your-cluster-name>-m`.

```
gcloud compute ssh <cluster-master-host> \
    --zone europe-west1-d \
    --ssh-flag="-D" \
    --ssh-flag="1080" \
    --ssh-flag="-N" \
    --ssh-flag="-f"
```

### Step 2: Open Chrome with special proxy settings

Open Chrome with a new session and tell it to use the SSH tunnel we just created.

On Windows:
```
C:\Program Files (x86)\Google\Chrome\Application\chrome.exe \
  --proxy-server="socks5://localhost:1080" \
  --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" \
  --user-data-dir=/tmp/master-host-name
```

On macOS:
```
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
    --user-data-dir="$HOME/chrome-proxy-profile" \
    --proxy-server="socks5://localhost:1080"
```

You should now be able to access the cluster interfaces in the Chrome window which just opened. Replace `<cluster-master-host>` with the name your your master hostname.

- Yarn cluster manager: `http://<cluster-master-host>:8088` releveant for both Hadoop MapReduce and Spark exercises.
- Spark: `http://<cluster-master-host>:18080`