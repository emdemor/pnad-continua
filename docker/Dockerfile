FROM jupyter/r-notebook

WORKDIR /app

COPY ./project .project

USER root
RUN chown -R jovyan:users /app

USER jovyan

EXPOSE 8888

RUN Rscript -e "install.packages('PNADcIBGE',repos='http://cran.us.r-project.org')"
RUN Rscript -e "install.packages('convey',repos='http://cran.us.r-project.org')"
RUN pip install pandas
RUN pip install xlrd
RUN pip install loguru
RUN pip install matplotlib
RUN pip install pyarrow
RUN pip install fastparquet

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''", "--NotebookApp.default_url='/lab/tree'", "--notebook-dir=/app/project"]