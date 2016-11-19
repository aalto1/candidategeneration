package PredictiveIndex;

/**
 * Created by aalto on 6/23/16.
 */

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class ReadWarcSample {

    public static void main(String[] args) throws IOException {
        String inputWarcFile="/home/aalto/IdeaProjects/PredictiveIndex/data/00.warc.gz";
        // open our gzip input stream
        GZIPInputStream gzInputStream=new GZIPInputStream(new FileInputStream(inputWarcFile));

        // cast to a data input stream
        DataInputStream inStream=new DataInputStream(gzInputStream);

        // iterate through our stream
        WarcRecord thisWarcRecord;
        //System.out.println(WarcRecord.readNextWarcRecord(inStream));
        while ((thisWarcRecord=WarcRecord.readNextWarcRecord(inStream))!=null) {
            // see if it's a response record
            if (thisWarcRecord.getHeaderRecordType().equals("response")) {
                // it is - create a WarcHTML record
                WarcHTMLResponseRecord htmlRecord=new WarcHTMLResponseRecord(thisWarcRecord);
                // get our TREC ID and target URI
                String thisTRECID=htmlRecord.getTargetTrecID();
                String thisTargetURI=htmlRecord.getTargetURI();
                String aux = htmlRecord.getRawRecord().getContentUTF8();
                String text = HtmlpageCleaner.startProcess(aux);
            }
        }

        inStream.close();
    }

}