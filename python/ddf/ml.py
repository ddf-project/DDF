from __future__ import unicode_literals

from py4j import java_gateway

from ml_models import KMeansModel


def kmeans(data, centers=2, runs=5, max_iters=10):
    """
    Train Kmeans on a given DDF
    :param data: DDF
    :param centers: number of clusters
    :param runs: number of runs
    :param max_iters: number of iterations
    :return: an object of type KMeansModel
    """
    ml_obj = java_gateway.get_field(data._jddf, 'ML')
    return KMeansModel(ml_obj.KMeans(centers, runs, max_iters), data._gateway_client, data.colnames)
