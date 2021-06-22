package ai.lum.odinson.lucene

import ai.lum.common.TryWithResources.using
import ai.lum.odinson.BuildInfo
import ai.lum.odinson.digraph.Vocabulary
import ai.lum.odinson.lucene.search.OdinsonIndexSearcher
import ai.lum.odinson.utils.IndexSettings
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.{Document => LuceneDocument}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{DirectoryReader, IndexReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.{IndexSearcher, Query, SearcherManager, TopDocs}
import org.apache.lucene.store.{Directory, IOContext}
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait LuceneIndex {
    protected val VOCABULARY_FILENAME = "dependencies.txt"
    protected val BUILDINFO_FILENAME = "buildinfo.json"
    protected val SETTINGSINFO_FILENAME = "settingsinfo.json"

    protected val directory : Directory

    protected val settings : IndexSettings
    protected val analyzer : Analyzer = new WhitespaceAnalyzer

    protected val computeTotalHits : Boolean = true

    val storedFields : Seq[ String ] = settings.storedFields
    val vocabulary = Vocabulary.fromDirectory( directory )

    def write( block : java.util.Collection[ LuceneDocument ] ) : Unit

    def search( query : Query, limit : Int = 1000000000 ) : TopDocs

    def numDocs( ) : Int

    def doc( docId : Int ) : LuceneDocument

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

class WriteOnceLuceneIndex( override protected val directory : Directory,
                            override protected val settings : IndexSettings ) extends LuceneIndex {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    private var isOpen : Boolean = true
    private lazy val reader : IndexReader = DirectoryReader.open( directory )
    private lazy val searcher : IndexSearcher = new OdinsonIndexSearcher( reader, computeTotalHits )
    private lazy val writer : IndexWriter = {
        val config = new IndexWriterConfig( this.analyzer )
        config.setOpenMode( OpenMode.CREATE )
        new IndexWriter( this.directory, config )
    }

    override def write( block : java.util.Collection[ LuceneDocument ] ) : Unit = {
        writer.addDocuments( block )
    }

    override def search( query : Query, limit : Int ) : TopDocs = {
        if ( !isOpen ) searcher.search( query, limit )
        else {
            LOG.error( "you cannot query a WriteOnceIndex while it is still open" )
            throw new IOException()
        }
    }

    override def numDocs( ) : Int = {
        if ( !isOpen ) reader.numDocs()
        else {
            LOG.error( "unable to open IndexReader for `numDocs` while a WriteOnceIndex is still open" )
            throw new IOException()
        }
    }

    override def doc( docId : Int ) : LuceneDocument = ???

    override def refresh( ) : Unit = LOG.warn( "calling `refresh` on a WriteOnceIndex does not do anything" )

    override def close( ) : Unit = {
        reader.close()
        isOpen = false
    }
}

class IncrementalLuceneIndex( override protected val directory : Directory,
                              override protected val settings : IndexSettings,
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

    override def write( block : java.util.Collection[ LuceneDocument ] ) : Unit = {
        writer.addDocuments( block )
        refresh()
    }

    override def doc( docId : Int ) : LuceneDocument = ???

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
}
