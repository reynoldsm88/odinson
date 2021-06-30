package ai.lum.odinson.lucene

import ai.lum.common.TryWithResources.using
import ai.lum.odinson.BuildInfo
import ai.lum.odinson.digraph.Vocabulary
import ai.lum.odinson.utils.IndexSettings
import ai.lum.odinson.utils.exceptions.OdinsonException
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.{Analyzer, TokenStream}
import org.apache.lucene.document.{Document, Document => LuceneDocument}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{Fields, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.highlight.TokenSources
import org.apache.lucene.search.{CollectorManager, IndexSearcher, Query, SearcherManager, TopDocs}
import org.apache.lucene.store.{Directory, IOContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait LuceneIndex {

    val computeTotalHits : Boolean

    protected val VOCABULARY_FILENAME = "dependencies.txt"
    protected val BUILDINFO_FILENAME = "buildinfo.json"
    protected val SETTINGSINFO_FILENAME = "settingsinfo.json"

    val directory : Directory
    val settings : IndexSettings
    val analyzer : Analyzer = new WhitespaceAnalyzer

    val storedFields : Seq[ String ] = settings.storedFields
    val vocabulary = Vocabulary.fromDirectory( directory )

    def write( block : java.util.Collection[ LuceneDocument ] ) : Unit

    def search( query : Query, limit : Int = 1000000000 ) : TopDocs

    def search[ CollectorType, ResultType ]( query : Query, manager : CollectorManager[ CollectorType, ResultType ] ) : ResultType

    def numDocs( ) : Int

    def doc( docId : Int ) : LuceneDocument

    def doc( docID : Int, fieldNames : Set[ String ] ) : LuceneDocument

    def getTermVectors( docId : Int ) : Fields

    def getTokens( doc : Document, termVectors : Fields, fieldName : String ) : Array[ String ]

    def refresh( ) : Unit

    def checkpoint( ) : Unit = {
        if ( directory.listAll().contains( VOCABULARY_FILENAME ) ) directory.deleteFile( VOCABULARY_FILENAME )
        if ( directory.listAll().contains( BUILDINFO_FILENAME ) ) directory.deleteFile( BUILDINFO_FILENAME )
        if ( directory.listAll().contains( SETTINGSINFO_FILENAME ) )
            directory.deleteFile( SETTINGSINFO_FILENAME )

        // FIXME: is this the correct instantiation of IOContext?
        using( directory.createOutput( VOCABULARY_FILENAME, new IOContext ) ) { stream =>
            stream.writeString( vocabulary.dump )
        }
        using( directory.createOutput( BUILDINFO_FILENAME, new IOContext ) ) { stream =>
            stream.writeString( BuildInfo.toJson )
        }
        using( directory.createOutput( SETTINGSINFO_FILENAME, new IOContext ) ) { stream =>
            stream.writeString( settings.dump )
        }
    }

    def close( ) : Unit

}

class IncrementalLuceneIndex( override val directory : Directory,
                              override val settings : IndexSettings,
                              override val computeTotalHits : Boolean,
                              protected val refreshMs : Long = 1000 ) extends LuceneIndex {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    private implicit val ec : ExecutionContext = ExecutionContext.global

    private val writer : IndexWriter = {
        val config = new IndexWriterConfig( this.analyzer )
        config.setOpenMode( OpenMode.CREATE_OR_APPEND )
        new IndexWriter( this.directory, config )
    }

    private val manager : SearcherManager = new SearcherManager( writer, new OdinsonSearcherFactory( computeTotalHits ) )

    refreshPeriodically()

    override def search( query : Query, limit : Int ) : TopDocs = {
        var searcher : IndexSearcher = null
        try {
            searcher = acquireSearcher()
            searcher.search( query, limit )
        } catch {
            case e : Throwable => throw new RuntimeException( "what is the best way to deal with this?" )
        }
        finally releaseSearcher( searcher )
    }

    override def search[ CollectorType, ResultType ]( query : Query, manager : CollectorManager[ CollectorType, ResultType ] ) : ResultType = {
        var searcher : IndexSearcher = null
        try {
            searcher = acquireSearcher()
            searcher.search[ CollectorType, ResultType ]( query, manager )
        } catch {
            case e : Throwable => throw new RuntimeException( "what is the best way to deal with this?" )
        }
        finally releaseSearcher( searcher )
    }

    override def write( block : java.util.Collection[ LuceneDocument ] ) : Unit = {
        writer.addDocuments( block )
        refresh()
    }

    override def doc( docId : Int ) : LuceneDocument = ???

    def doc( docId : Int, fieldNames : Set[ String ] ) : LuceneDocument = {
        var searcher : IndexSearcher = null
        try {
            searcher = acquireSearcher()
            searcher.getIndexReader.document( docId, fieldNames.asJava )
        } catch {
            case e : Throwable => throw new RuntimeException( "what is the best way to deal with this?" )
        }
        finally releaseSearcher( searcher )
    }

    override def getTermVectors( docId : Int ) : Fields = {
        var searcher : IndexSearcher = null
        try {
            searcher = acquireSearcher()
            searcher.getIndexReader.getTermVectors( docId )
        } catch {
            case e : Throwable => throw new RuntimeException( "what is the best way to deal with this?" )
        }
        finally releaseSearcher( searcher )
    }

    override def refresh( ) : Unit = {
        writer.flush()
        writer.commit()
        manager.maybeRefresh()
    }

    override def numDocs( ) : Int = {
        var searcher : IndexSearcher = null
        try {
            searcher = acquireSearcher()
            searcher.getIndexReader.numDocs()
        } catch {
            case e : Throwable => throw new RuntimeException( "what is the best way to deal with this?" )
        }
        finally releaseSearcher( searcher )
    }

    private def acquireSearcher( ) : IndexSearcher = manager.acquire()

    private def releaseSearcher( searcher : IndexSearcher ) : Unit = manager.release( searcher )

    private def refreshPeriodically( ) : Unit = {
        Future {
            println( "refreshing index searchers with updated data" )
            Thread.sleep( refreshMs )
            refresh()
        } onComplete {
            case Success( _ ) => refreshPeriodically()
            case Failure( e : Throwable ) => ???
        }
    }

    def close( ) : Unit = {}

    override def getTokens( doc : Document,
                            termVectors : Fields,
                            fieldName : String ) : Array[ String ] = {

        val field = doc.getField( fieldName )
        if ( field == null ) throw new OdinsonException( s"Attempted to getTokens from field that was not stored: $fieldName" )
        val text = field.stringValue
        val ts = TokenSources.getTokenStream( fieldName, termVectors, text, analyzer, -1 )
        val tokens = getTokens( ts )
        tokens
    }

    private def getTokens( ts : TokenStream ) : Array[ String ] = {
        ts.reset()
        val terms = new ArrayBuffer[ String ]

        while ( ts.incrementToken() ) {
            val charTermAttribute = ts.addAttribute( classOf[ CharTermAttribute ] )
            val term = charTermAttribute.toString
            terms += term
        }

        ts.end()
        ts.close()

        terms.toArray
    }
}
