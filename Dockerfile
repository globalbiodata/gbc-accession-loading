FROM continuumio/miniconda3

ADD environment_full.yaml /tmp/environment.yaml
RUN conda env create -f /tmp/environment.yaml

RUN echo "conda activate gbc-conda-full" >> ~/.bashrc
ENV PATH=/opt/conda/envs/gbc-conda-full/bin:$PATH

# download python module extra requirements
RUN python -m spacy download en_core_web_sm
RUN python -m nltk.downloader -d /usr/local/share/nltk_data punkt_tab
RUN python -m nltk.downloader -d /usr/local/share/nltk_data averaged_perceptron_tagger_eng
RUN python -m nltk.downloader -d /usr/local/share/nltk_data maxent_ne_chunker_tab
RUN python -m nltk.downloader -d /usr/local/share/nltk_data words
ENV NLTK_DATA=/usr/local/share/nltk_data

# fetch shared globalbiodata utilities from central repo
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 https://github.com/globalbiodata/gbc-publication-analysis.git /opt/gbc-publication-analysis
ENV PYTHONPATH=/opt/gbc-publication-analysis:$PYTHONPATH