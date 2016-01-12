from __future__ import unicode_literals

import pandas as pd


class Model(object):

    def __init__(self, jml_model):
        self._jml_model = jml_model

    def predict(self, data):
        """
        Predict the result of a sample using this ML model

        :param data:  the candidate sample data to be predicted, vector is expected
        :return: predict result, class tag for classification,
        """
        return self._jml_model.predict(data)


class KMeansModel(Model):

    def __init__(self, jml_model, col_names):
        super(KMeansModel, self).__init__(jml_model)
        self.centers = pd.DataFrame(data=[list(x.toArray()) for x in self._jml_model.getRawModel().clusterCenters()],
                                    columns=col_names)

    def summary(self, print_out=True):
        s = 'KMeans model with {} centers:\n\n{}'.format(len(self.centers), self.centers)
        if print_out:
            print s
        else:
            return s

    def __repr__(self):
        return 'KMeansModel({} clusters)'.format(len(self.centers))

    def __str__(self):
        return 'KMeansModel({} clusters)'.format(len(self.centers))
