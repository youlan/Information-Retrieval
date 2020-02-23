

import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.disk.DiskLengthsReader;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;


import java.util.Set;
import java.util.TreeSet;
import java.io.File;




public class indexing {
	
	public static void main(String[] args) {
		
		String pathIndexBase = "/Users/dchen/Documents/2020Spring/IR/hw/hw1/indexing/";
		String field = "text";	

		try {
			int totalLengths = 0;
			String maxLDocId = "";
			int maxLValue = 0;
			int totalDocs = 0;
			File fileLength = new File( new File( pathIndexBase ), "lengths" );
			Retrieval retrieval = RetrievalFactory.instance( pathIndexBase );
			LengthsReader indexLength = new DiskLengthsReader( fileLength.getAbsolutePath() );
			Node query = StructuredQuery.parse( "#lengths:" + field + ":part=lengths()" );
			
			LengthsIterator iterator = (LengthsIterator) indexLength.getIterator( query );
			ScoringContext sc = new ScoringContext();
			
			while ( !iterator.isDone() ) {
				sc.document = iterator.currentCandidate();
				String docno = retrieval.getDocumentName( (long) sc.document );
				int length = iterator.length( sc );
				if (length > maxLValue) {
					maxLValue = length;
					maxLDocId = docno;
					
				}
				totalLengths += length;
				totalDocs += 1;
				
				iterator.movePast( iterator.currentCandidate() );
			}
			
			indexLength.close();
			retrieval.close();	
			System.out.printf("1. Total number of documents is %d\n", totalDocs);
			System.out.printf("2. Average length of documents is %.2f\n", (double) totalLengths / totalDocs);
			System.out.printf("4. Document: %s, its length is %d\n", maxLDocId, maxLValue);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			Retrieval retrieval = RetrievalFactory.instance(pathIndexBase);
			Node termNode = StructuredQuery.parse( "#lengths:" + field + ":part=lengths()" );
			
			FieldStatistics docStats = retrieval.getCollectionStatistics(termNode);
			
			Set<String> docVector = new TreeSet<>();
			for ( long docid = docStats.firstDocId; docid <= docStats.lastDocId; docid++ ) {
				Document.DocumentComponents dc = new Document.DocumentComponents( false, false, true );
				Document doc = retrieval.getDocument( retrieval.getDocumentName( docid ), dc );
				for ( Tag tag : doc.tags ) { 
				    if ( tag.name.equals( field ) ) {
				        for ( int position = tag.begin; position < tag.end; position++ ) {
				            String term = doc.terms.get( position );
				            docVector.add(term);
				        }
				    }
				}
				
			}		
			retrieval.close();
			System.out.printf("3. There are %d unique words in the corpus.\n", docVector.size());
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			int count = 0;
			String term = "information";
			File pathPosting = new File(new File(pathIndexBase), "field." + field);
			DiskIndex index = new DiskIndex(pathIndexBase);
			IndexPartReader posting = DiskIndex.openIndexPart(pathPosting.getAbsolutePath());
			
			KeyIterator vocabulary = posting.getIterator();
			if (vocabulary.skipToKey(ByteUtil.fromString(term)) && term.equals(vocabulary.getKeyString())) {
				 
				CountIterator iterator = (CountIterator) vocabulary.getValueIterator();
				ScoringContext sc = new ScoringContext();
				 
				 while (!iterator.isDone()) { 
					 sc.document = iterator.currentCandidate();
					 count += 1;
					 iterator.movePast(iterator.currentCandidate());					 
				 }
			}		 
			 posting.close();
			 index.close();
			 System.out.printf("5. There are %d documents contained the word information.\n ", count);
			
		} catch(Exception e) {
			e.printStackTrace();
		}		
	}

}

