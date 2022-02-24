package models;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class SensorEventBuilder {

    public static SensorEvent buildEvent(String xmlEvent) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(SensorEvent.class);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            SensorEvent sensor = (SensorEvent) jaxbUnmarshaller.unmarshal(new StringReader(xmlEvent));

            return sensor;
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static  TableSchema getBQTableSchema() {

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("timestamp").setType("STRING"));
        fields.add(new TableFieldSchema().setName("latitude").setType("STRING"));
        fields.add(new TableFieldSchema().setName("longitude").setType("STRING"));
        fields.add(new TableFieldSchema().setName("highway").setType("STRING"));
        fields.add(new TableFieldSchema().setName("direction").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lane").setType("STRING"));
        fields.add(new TableFieldSchema().setName("speed").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sensorId").setType("STRING"));

       return  new TableSchema().setFields(fields);
    }

    public static TableRow toBQRow(SensorEvent event) {

        TableRow row = new TableRow();
        row.set("timestamp", event.getTimestamp());
        row.set("latitude", event.getLatitude());
        row.set("longitude", event.getLongitude());
        row.set("highway", event.getHighway());
        row.set("direction", event.getDirection());
        row.set("lane", event.getLane());
        row.set("speed", event.getSpeed());
        row.set("sensorId", event.getId());

        return row;
    }






}
