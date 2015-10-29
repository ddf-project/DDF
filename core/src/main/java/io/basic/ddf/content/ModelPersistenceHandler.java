package io.basic.ddf.content;

import io.ddf.ml.IModel;
import io.ddf.misc.Config;
import io.ddf.util.Utils;
import io.ddf.exception.DDFException;
import java.io.IOException;

/**
 * Created by steveyan on 10/28/15.
 */
public class ModelPersistenceHandler {
    private IModel model;

    public ModelPersistenceHandler(IModel model){
        this.model = model;
    }

    /**
     * find the directory in hdfs for model: hdfs:ddf-runtime/basic-model-db
     * @return
     * @throws DDFException
     */
    public static String locateOrCreateModelPersistenceDirectory() throws DDFException {
        String result;

        try {
            result = Utils.locateOrCreateDirectory(Config.getBasicModelPersistenceDir());

        } catch (Exception e) {
            throw new DDFException(String.format("Unable to getModelPersistenceDirectory"), e);
        }

        return result;
    }

    /**
     * find the full directory in hdfs for model: hdfs:ddf-runtime/basic-model-db/adatao
     * @param subdir
     * @return
     * @throws DDFException
     */
    public static String locateOrCreateModelPersistenceSubdirectory(String subdir) throws DDFException {
        String result = null, path = null;

        try {
            path = String.format("%s/%s", locateOrCreateModelPersistenceDirectory(), subdir);
            result = Utils.locateOrCreateDirectory(path);

        } catch (Exception e) {
            throw new DDFException(String.format("Unable to getModelPersistenceSubdirectory(%s)", path), e);
        }

        return result;
    }

    /**
     * find the full model file in hdfs for model: hdfs:ddf-runtime/basic-model-db/adatao/some_uuid.model
     * @param modelName
     * @return
     * @throws DDFException
     */
    public static String getModelFileFullName(String modelName) throws DDFException {
        String namespace = Config.getGlobalValue("Namespace");
        String directory = locateOrCreateModelPersistenceSubdirectory(namespace);
        String postfix = ".model";

        return String.format("%s/%s%s", directory, modelName, postfix);
    }

    public static String getModelFileFullName(IModel model) throws DDFException {
        return getModelFileFullName(model.getName());
    }

    public static void persistModel(IModel model, boolean overwrite) throws DDFException {

        String modelFile = getModelFileFullName(model);

        try {
            if (!overwrite && (Utils.fileExists(modelFile))) {
                throw new DDFException("Model file already exists in persistence storage, and overwrite option is false");
            }
        } catch (IOException e) {
            throw new DDFException(e);
        }

        try {
            //if overwrite and existed
            if (overwrite && Utils.fileExists(modelFile)) {
                Utils.deleteFile(modelFile);
            }

            Utils.writeObjectToFile(modelFile, model);
        } catch (Exception e) {
            if (e instanceof DDFException) throw (DDFException) e;
            else throw new DDFException(e);
        }
    }

    public static IModel getModelFromFile(String modelName) throws DDFException, IOException, ClassNotFoundException {
        IModel model = (IModel) Utils.readFromObjectFile(getModelFileFullName(modelName));
        return model;
    }
}
