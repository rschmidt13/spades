--default INPUT 'wat/WEB-20130124130848014-00000-31940~s3scape01~8083.wat.gz';
%default INPUT 'wat';
%default OUTPUT 'alllinks';
%default OUTPUT_Sources 'alllinks_sources';
%default OUTPUT_ULinks 'alllinks_ulinks';


SET pig.splitCombination 'false';

-- REGISTER archive-meta-extractor-20110512.jar;
REGISTER archive-meta-extractor-20110609.jar;

-- alias short-hand for IA 'resolve()' UDF:
DEFINE resolve org.archive.hadoop.func.URLResolverFunc();

-- load data from INPUT:
Orig = LOAD '$INPUT' USING org.archive.hadoop.ArchiveJSONViewLoader('Envelope.WARC-Header-Metadata.WARC-Target-URI','Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Head.Base','Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.@Links.{url,path,text,alt}') AS (src:chararray,html_base:chararray,relative:chararray,path:chararray,text:chararray,alt:chararray);

-- converts relative URL to absolute URL
ResolvedLinks = FOREACH LinksOnly GENERATE src, resolve(src,html_base,relative) AS dst, path, CONCAT(text,alt) AS linktext;
-- SortedLinks = ORDER ResolvedLinks BY src, dst, path, linktext;

AllLinks = FOREACH ResolvedLinks GENERATE dst AS links;

-- Filter by Domain
-- TODO provide a more generic mechanism to filter scope of crawl
-- TODO write an UDF for concating multiple strings; FILTER = CONCAT('regex1', $DOMAN, 'regex2');
AllLinks = FILTER AllLinks by links MATCHES '^(http:\\/\\/)?([\\w\\d\\-]*\\.)*python.org(:[0-9]+)?.*';

-- Cononicalize urls, trim, trailing slashes, hashes, etc. (without discarding these urls)
AllLinks = FOREACH AllLinks { b = REGEX_EXTRACT($0,'(.*)(#.*$)', 1); GENERATE ((b is null) ? $0 : b ); };
AllLinks = FOREACH AllLinks { c = REGEX_EXTRACT($0,'(.*)([#\\/\\?]$)', 1); GENERATE ((c is null) ? $0 : c); };

-- Deduplicate links
XAllLinks = DISTINCT AllLinks;

AllSources = FOREACH Orig GENERATE src AS sources;
AllSources = FILTER AllSources by sources MATCHES '^(http:\\/\\/)?([\\w\\d\\-]*\\.)*python.org(:[0-9]+)?.*';
AllSources = FOREACH AllSources { b = REGEX_EXTRACT($0,'(.*)(#.*$)', 1); GENERATE ((b is null) ? $0 : b ); };
AllSources = FOREACH AllSources { c = REGEX_EXTRACT($0,'(.*)([#\\/\\?]$)', 1); GENERATE ((c is null) ? $0 : c); };
XAllSources = DISTINCT AllSources;

LinksToSources = JOIN XAllLinks by $0 LEFT OUTER, XAllSources by $0;
LinksNoSources = FILTER LinksToSources BY $1 is null;
STORE LinksToSources INTO '$OUTPUT';
STORE LinksNoSources INTO '$OUTPUT_ULinks';
STORE XAllSources INTO '$OUTPUT_Sources';


-- TODO try to resolve un-craweld links, might refer (using via query string) to an already crawled resource
-- TOD0 look for urls shorteners and resolve links before testing for completeness   

-- Count number of lines of two tables 
-- XAllLinksGroup = GROUP XAllLinks ALL;
-- XAllSourcesGroup = GROUP XAllSources ALL;
-- somehow, it is not possible to assign an alias to the constant "1"
-- XAllLinksCount = FOREACH XAllLinksGroup GENERATE 1, COUNT(XAllLinks) AS sum_links;
-- XAllSourcesCount = FOREACH XAllSourcesGroup GENERATE 1, COUNT(XAllSources) As sum_sources;
-- SumResult = JOIN XAllSourcesCount BY $0, XAllLinksCount BY $0;
-- Store SumResult INTO '$OUTPUT';

