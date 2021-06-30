package ai.lum.odinson.lucene.analysis

import ai.lum.odinson.lucene.LuceneIndex
import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexReader

import scala.collection.JavaConverters._

object TokenStreamUtils {

    def getDoc( docID : Int, fieldNames : Set[ String ], indexReader : IndexReader ) : Document = {
        indexReader.document( docID, fieldNames.asJava )
    }

    def getTokensFromMultipleFields( docID : Int, fieldNames : Set[ String ], luceneIndex : LuceneIndex ) : Map[ String, Array[ String ] ] = {
        val doc = luceneIndex.doc( docID, fieldNames )
        val termVectors = luceneIndex.getTermVectors( docID )
        fieldNames
          .map( field => (field, luceneIndex.getTokens( doc, termVectors, field )) )
          .toMap
    }

}
