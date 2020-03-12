package bigt;

import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.IOException;

/**
 * This heapfile implementation is directory-based. We maintain a
 * directory of info about the dfata pages (which are of type HFPage
 * when loaded into memory).  The directory itself is also composed
 * of HFPages, with each record being of type DataPageInfo
 * as defined below.
 * <p>
 * The first directory page is a header page for the entire database
 * (it is the one to which our filename is mapped by the DB).
 * All directory pages are in a doubly-linked list of pages, each
 * directory entry points to a single data page, which contains
 * the actual records.
 * <p>
 * The heapfile data pages are implemented as slotted pages, with
 * the slots at the front and the records in the back, both growing
 * into the free space in the middle of the page.
 * <p>
 * We can store roughly pagesize/sizeof(DataPageInfo) records per
 * directory page; for any given HeapFile insertion, it is likely
 * that at least one of those referenced data pages will have
 * enough free space to satisfy the request.
 */


/**
 * DataPageInfo class : the type of records stored on a directory page.
 * <p>
 * April 9, 1998
 */


interface Filetype {
    int TEMP = 0;
    int ORDINARY = 1;

} // end of Filetype

public class BigT implements Filetype, GlobalConst {

    private String name;
    private int type;

    PageId _firstDirPageId;   // page number of header page
    int _ftype;
    private boolean _file_deleted;
    private String _fileName;
    private static int tempfilecount = 0;


    /* get a new datapage from the buffer manager and initialize dpinfo
       @param dpinfop the information in the new HFPage
    */
    private HFPage _newDatapage(DataPageInfo dpinfop)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {
        Page apage = new Page();
        PageId pageId = new PageId();
        pageId = newPage(apage, 1);

        if (pageId == null)
            throw new HFException(null, "can't new pae");

        // initialize internal values of the new page:

        HFPage hfpage = new HFPage();
        hfpage.init(pageId, apage);

        dpinfop.pageId.pid = pageId.pid;
        dpinfop.recct = 0;
        dpinfop.availspace = hfpage.available_space();

        return hfpage;

    } // end of _newDatapage

    /* Internal HeapFile function (used in getRecord and updateRecord):
       returns pinned directory page and pinned data page of the specified
       user record(rid) and true if record is found.
       If the user record cannot be found, return false.
    */
    private boolean _findDataPage(RID rid,
                                  PageId dirPageId, HFPage dirpage,
                                  PageId dataPageId, HFPage datapage,
                                  RID rpDataPageRid)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            Exception {
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);

        HFPage currentDirPage = new HFPage();
        HFPage currentDataPage = new HFPage();
        RID currentDataPageRid = new RID();
        PageId nextDirPageId = new PageId();
        // datapageId is stored in dpinfo.pageId


        pinPage(currentDirPageId, currentDirPage, false/*read disk*/);

        Map aMap = new Map();

        while (currentDirPageId.pid != INVALID_PAGE) {// Start While01
            // ASSERTIONS:
            //  currentDirPage, currentDirPageId valid and pinned and Locked.

            for (currentDataPageRid = currentDirPage.firstMap();
                 currentDataPageRid != null;
                 currentDataPageRid = currentDirPage.nextMap(currentDataPageRid)) {
                try {
                    aMap = currentDirPage.getMap(currentDataPageRid);
                } catch (InvalidSlotNumberException e)// check error! return false(done)
                {
                    return false;
                }

                DataPageInfo dpinfo = new DataPageInfo(aMap);
                try {
                    pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/);


                    //check error;need unpin currentDirPage
                } catch (Exception e) {
                    unpinPage(currentDirPageId, false/*undirty*/);
                    dirpage = null;
                    datapage = null;
                    throw e;
                }


                // ASSERTIONS:
                // - currentDataPage, currentDataPageRid, dpinfo valid
                // - currentDataPage pinned

                if (dpinfo.pageId.pid == rid.pageNo.pid) {
                    aMap = currentDataPage.returnMap(rid);
                    // found user's record on the current datapage which itself
                    // is indexed on the current dirpage.  Return both of these.

                    dirpage.setpage(currentDirPage.getpage());
                    dirPageId.pid = currentDirPageId.pid;

                    datapage.setpage(currentDataPage.getpage());
                    dataPageId.pid = dpinfo.pageId.pid;

                    rpDataPageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
                    rpDataPageRid.slotNo = currentDataPageRid.slotNo;
                    return true;
                } else {
                    // user record not found on this datapage; unpin it
                    // and try the next one
                    unpinPage(dpinfo.pageId, false /*undirty*/);

                }

            }

            // if we would have found the correct datapage on the current
            // directory page we would have already returned.
            // therefore:
            // read in next directory page:

            nextDirPageId = currentDirPage.getNextPage();
            try {
                unpinPage(currentDirPageId, false /*undirty*/);
            } catch (Exception e) {
                throw new HFException(e, "heapfile,_find,unpinpage failed");
            }

            currentDirPageId.pid = nextDirPageId.pid;
            if (currentDirPageId.pid != INVALID_PAGE) {
                pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
                if (currentDirPage == null)
                    throw new HFException(null, "pinPage return null page");
            }
        } // end of While01
        // checked all dir pages and all data pages; user record not found:(

        dirPageId.pid = dataPageId.pid = INVALID_PAGE;

        return false;


    } // end of _findDatapage

    /**
     * Initialize.  A null name produces a temporary heapfile which will be
     * deleted by the destructor.  If the name already denotes a file, the
     * file is opened; otherwise, a new empty file is created.
     *
     * @throws HFException        heapfile exception
     * @throws HFBufMgrException  exception thrown from bufmgr layer
     * @throws HFDiskMgrException exception thrown from diskmgr layer
     * @throws IOException        I/O errors
     */
    public BigT(String name, int type)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {

        this.name = name;
        this.type = type;

        // Give us a prayer of destructing cleanly if construction fails.
        _file_deleted = true;
        _fileName = null;

        if (name == null) {
            // If the name is NULL, allocate a temporary name
            // and no logging is required.
            _fileName = "tempBigTableFile";
            String useId = new String("user.name");
            String userAccName;
            userAccName = System.getProperty(useId);
            _fileName = _fileName + userAccName;

            String filenum = Integer.toString(tempfilecount);
            _fileName = _fileName + filenum;
            _ftype = TEMP;
            tempfilecount++;

        } else {
            _fileName = name + type;
            _ftype = ORDINARY;
        }

        // The constructor gets run in two different cases.
        // In the first case, the file is new and the header page
        // must be initialized.  This case is detected via a failure
        // in the db->get_file_entry() call.  In the second case, the
        // file already exists and all that must be done is to fetch
        // the header page into the buffer pool

        // try to open the file

        Page apage = new Page();
        _firstDirPageId = null;
        if (_ftype == ORDINARY)
            _firstDirPageId = get_file_entry(_fileName);

        if (_firstDirPageId == null) {
            // file doesn't exist. First create it.
            _firstDirPageId = newPage(apage, 1);
            // check error
            if (_firstDirPageId == null)
                throw new HFException(null, "can't new page");

            add_file_entry(_fileName, _firstDirPageId);
            // check error(new exception: Could not add file entry

            HFPage firstDirPage = new HFPage();
            firstDirPage.init(_firstDirPageId, apage);
            PageId pageId = new PageId(INVALID_PAGE);

            firstDirPage.setNextPage(pageId);
            firstDirPage.setPrevPage(pageId);
            unpinPage(_firstDirPageId, true /*dirty*/);


        }
        _file_deleted = false;
        // ASSERTIONS:
        // - ALL private data members of class Heapfile are valid:
        //
        //  - _firstDirPageId valid
        //  - _fileName valid
        //  - no datapage pinned yet

    } // end of constructor

    /**
     * Return number of records in file.
     *
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws IOException                I/O errors
     */
    public int getMapCnt()
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException {
        int answer = 0;
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);

        PageId nextDirPageId = new PageId(0);

        HFPage currentDirPage = new HFPage();
        Page pageinbuffer = new Page();

        while (currentDirPageId.pid != INVALID_PAGE) {
            pinPage(currentDirPageId, currentDirPage, false);

            RID rid = new RID();
            Map aMap;
            for (rid = currentDirPage.firstMap();
                 rid != null;    // rid==NULL means no more record
                 rid = currentDirPage.nextMap(rid)) {
                aMap = currentDirPage.getMap(rid);
                DataPageInfo dpinfo = new DataPageInfo(aMap);

                answer += dpinfo.recct;
            }

            // ASSERTIONS: no more record
            // - we have read all datapage records on
            //   the current directory page.

            nextDirPageId = currentDirPage.getNextPage();
            unpinPage(currentDirPageId, false /*undirty*/);
            currentDirPageId.pid = nextDirPageId.pid;
        }

        // ASSERTIONS:
        // - if error, exceptions
        // - if end of heapfile reached: currentDirPageId == INVALID_PAGE
        // - if not yet end of heapfile: currentDirPageId valid


        return answer;
    } // end of getRecCnt

    // Return number of distinct row labels in the bigtable
    public int getRowCnt() {
        return 0;
    }

    // Return number of distinct column labels in the bigtable
    public int getColumnCnt() {
        return 0;
    }

    /**
     * Insert record into file, return its Rid.
     *
     * @param mapPtr pointer of the record
     * @return the rid of the record
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws SpaceNotAvailableException no space left
     * @throws HFException                heapfile exception
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws IOException                I/O errors
     */
    public RID insertMap(byte[] mapPtr)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {
        int dpinfoLen = 0;
        int recLen = mapPtr.length;
        boolean found;
        RID currentDataPageRid = new RID();
        Page pageinbuffer = new Page();
        HFPage currentDirPage = new HFPage();
        HFPage currentDataPage = new HFPage();

        HFPage nextDirPage = new HFPage();
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        PageId nextDirPageId = new PageId();  // OK

        pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);

        found = false;
        Map aMap;
        DataPageInfo dpinfo = new DataPageInfo();
        while (found == false) { //Start While01
            // look for suitable dpinfo-struct
            for (currentDataPageRid = currentDirPage.firstMap();
                 currentDataPageRid != null;
                 currentDataPageRid =
                         currentDirPage.nextMap(currentDataPageRid)) {
                aMap = currentDirPage.getMap(currentDataPageRid);

                dpinfo = new DataPageInfo(aMap);

                // need check the record length == DataPageInfo'slength

                if (recLen <= dpinfo.availspace) {
                    found = true;
                    break;
                }
            }

            // two cases:
            // (1) found == true:
            //     currentDirPage has a datapagerecord which can accomodate
            //     the record which we have to insert
            // (2) found == false:
            //     there is no datapagerecord on the current directory page
            //     whose corresponding datapage has enough space free
            //     several subcases: see below
            if (found == false) { //Start IF01
                // case (2)

                //System.out.println("no datapagerecord on the current directory is OK");
                //System.out.println("dirpage availspace "+currentDirPage.available_space());

                // on the current directory page is no datapagerecord which has
                // enough free space
                //
                // two cases:
                //
                // - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
                //         if there is enough space on the current directory page
                //         to accomodate a new datapagerecord (type DataPageInfo),
                //         then insert a new DataPageInfo on the current directory
                //         page
                // - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
                //         look at the next directory page, if necessary, create it.

                if (currentDirPage.available_space() >= dpinfo.size) {
                    //Start IF02
                    // case (2.1) : add a new data page record into the
                    //              current directory page
                    currentDataPage = _newDatapage(dpinfo);
                    // currentDataPage is pinned! and dpinfo->pageId is also locked
                    // in the exclusive mode

                    // didn't check if currentDataPage==NULL, auto exception


                    // currentDataPage is pinned: insert its record
                    // calling a HFPage function


                    aMap = dpinfo.convertToMap();

                    byte[] tmpData = aMap.getMapByteArray();
                    currentDataPageRid = currentDirPage.insertMap(tmpData);

                    RID tmprid = currentDirPage.firstMap();


                    // need catch error here!
                    if (currentDataPageRid == null)
                        throw new HFException(null, "no space to insert rec.");

                    // end the loop, because a new datapage with its record
                    // in the current directorypage was created and inserted into
                    // the heapfile; the new datapage has enough space for the
                    // record which the user wants to insert

                    found = true;

                } //end of IF02
                else {  //Start else 02
                    // case (2.2)
                    nextDirPageId = currentDirPage.getNextPage();
                    // two sub-cases:
                    //
                    // (2.2.1) nextDirPageId != INVALID_PAGE:
                    //         get the next directory page from the buffer manager
                    //         and do another look
                    // (2.2.2) nextDirPageId == INVALID_PAGE:
                    //         append a new directory page at the end of the current
                    //         page and then do another loop

                    if (nextDirPageId.pid != INVALID_PAGE) { //Start IF03
                        // case (2.2.1): there is another directory page:
                        unpinPage(currentDirPageId, false);

                        currentDirPageId.pid = nextDirPageId.pid;

                        pinPage(currentDirPageId,
                                currentDirPage, false);


                        // now go back to the beginning of the outer while-loop and
                        // search on the current directory page for a suitable datapage
                    } //End of IF03
                    else {  //Start Else03
                        // case (2.2): append a new directory page after currentDirPage
                        //             since it is the last directory page
                        nextDirPageId = newPage(pageinbuffer, 1);
                        // need check error!
                        if (nextDirPageId == null)
                            throw new HFException(null, "can't new pae");

                        // initialize new directory page
                        nextDirPage.init(nextDirPageId, pageinbuffer);
                        PageId temppid = new PageId(INVALID_PAGE);
                        nextDirPage.setNextPage(temppid);
                        nextDirPage.setPrevPage(currentDirPageId);

                        // update current directory page and unpin it
                        // currentDirPage is already locked in the Exclusive mode
                        currentDirPage.setNextPage(nextDirPageId);
                        unpinPage(currentDirPageId, true/*dirty*/);

                        currentDirPageId.pid = nextDirPageId.pid;
                        currentDirPage = new HFPage(nextDirPage);

                        // remark that MINIBASE_BM->newPage already
                        // pinned the new directory page!
                        // Now back to the beginning of the while-loop, using the
                        // newly created directory page.

                    } //End of else03
                } // End of else02
                // ASSERTIONS:
                // - if found == true: search will end and see assertions below
                // - if found == false: currentDirPage, currentDirPageId
                //   valid and pinned

            }//end IF01
            else { //Start else01
                // found == true:
                // we have found a datapage with enough space,
                // but we have not yet pinned the datapage:

                // ASSERTIONS:
                // - dpinfo valid

                // System.out.println("find the dirpagerecord on current page");

                pinPage(dpinfo.pageId, currentDataPage, false);
                //currentDataPage.openHFpage(pageinbuffer);


            }//End else01
        } //end of While01

        // ASSERTIONS:
        // - currentDirPageId, currentDirPage valid and pinned
        // - dpinfo.pageId, currentDataPageRid valid
        // - currentDataPage is pinned!

        if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
            throw new HFException(null, "invalid PageId");

        if (!(currentDataPage.available_space() >= recLen))
            throw new SpaceNotAvailableException(null, "no available space");

        if (currentDataPage == null)
            throw new HFException(null, "can't find Data page");


        RID rid;
        rid = currentDataPage.insertMap(mapPtr);

        dpinfo.recct++;
        dpinfo.availspace = currentDataPage.available_space();


        unpinPage(dpinfo.pageId, true /* = DIRTY */);

        // DataPage is now released
        aMap = currentDirPage.returnMap(currentDataPageRid);
        DataPageInfo dpinfo_ondirpage = new DataPageInfo(aMap);


        dpinfo_ondirpage.availspace = dpinfo.availspace;
        dpinfo_ondirpage.recct = dpinfo.recct;
        dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
        dpinfo_ondirpage.flushToMap();


        unpinPage(currentDirPageId, true /* = DIRTY */);


        return rid;

    }

    /**
     *
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @return Stream
     */
    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception{
        Stream stream = new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
        return stream;
    }

    /**
     * Delete record from file with given rid.
     *
     * @return true record deleted  false:record not found
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws HFException                heapfile exception
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws Exception                  other exception
     */
    public boolean deleteMap(RID rid)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            Exception {
        boolean status;
        HFPage currentDirPage = new HFPage();
        PageId currentDirPageId = new PageId();
        HFPage currentDataPage = new HFPage();
        PageId currentDataPageId = new PageId();
        RID currentDataPageRid = new RID();

        status = _findDataPage(rid,
                currentDirPageId, currentDirPage,
                currentDataPageId, currentDataPage,
                currentDataPageRid);

        if (status != true) return status;    // record not found

        // ASSERTIONS:
        // - currentDirPage, currentDirPageId valid and pinned
        // - currentDataPage, currentDataPageid valid and pinned

        // get datapageinfo from the current directory page:
        Map aMap;

        aMap = currentDirPage.returnMap(currentDataPageRid);
        DataPageInfo pdpinfo = new DataPageInfo(aMap);

        // delete the record on the datapage
        currentDataPage.deleteMap(rid);

        pdpinfo.recct--;
        pdpinfo.flushToMap();    //Write to the buffer pool
        if (pdpinfo.recct >= 1) {
            // more records remain on datapage so it still hangs around.
            // we just need to modify its directory entry

            pdpinfo.availspace = currentDataPage.available_space();
            pdpinfo.flushToMap();
            unpinPage(currentDataPageId, true /* = DIRTY*/);

            unpinPage(currentDirPageId, true /* = DIRTY */);


        } else {
            // the record is already deleted:
            // we're removing the last record on datapage so free datapage
            // also, free the directory page if
            //   a) it's not the first directory page, and
            //   b) we've removed the last DataPageInfo record on it.

            // delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
            unpinPage(currentDataPageId, false /*undirty*/);

            freePage(currentDataPageId);

            // delete corresponding DataPageInfo-entry on the directory page:
            // currentDataPageRid points to datapage (from for loop above)

            currentDirPage.deleteMap(currentDataPageRid);


            // ASSERTIONS:
            // - currentDataPage, currentDataPageId invalid
            // - empty datapage unpinned and deleted

            // now check whether the directory page is empty:

            currentDataPageRid = currentDirPage.firstMap();

            // st == OK: we still found a datapageinfo record on this directory page
            PageId pageId;
            pageId = currentDirPage.getPrevPage();
            if ((currentDataPageRid == null) && (pageId.pid != INVALID_PAGE)) {
                // the directory-page is not the first directory page and it is empty:
                // delete it

                // point previous page around deleted page:

                HFPage prevDirPage = new HFPage();
                pinPage(pageId, prevDirPage, false);

                pageId = currentDirPage.getNextPage();
                prevDirPage.setNextPage(pageId);
                pageId = currentDirPage.getPrevPage();
                unpinPage(pageId, true /* = DIRTY */);


                // set prevPage-pointer of next Page
                pageId = currentDirPage.getNextPage();
                if (pageId.pid != INVALID_PAGE) {
                    HFPage nextDirPage = new HFPage();
                    pageId = currentDirPage.getNextPage();
                    pinPage(pageId, nextDirPage, false);

                    //nextDirPage.openHFpage(apage);

                    pageId = currentDirPage.getPrevPage();
                    nextDirPage.setPrevPage(pageId);
                    pageId = currentDirPage.getNextPage();
                    unpinPage(pageId, true /* = DIRTY */);

                }

                // delete empty directory page: (automatically unpinned?)
                unpinPage(currentDirPageId, false/*undirty*/);
                freePage(currentDirPageId);


            } else {
                // either (the directory page has at least one more datapagerecord
                // entry) or (it is the first directory page):
                // in both cases we do not delete it, but we have to unpin it:

                unpinPage(currentDirPageId, true /* == DIRTY */);


            }
        }
        return true;
    }


    /**
     * Updates the specified record in the heapfile.
     *
     * @param rid:      the record which needs update
     * @param newMap: the new content of the record
     * @return ture:update success   false: can't find the record
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidUpdateException     invalid update on record
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws HFException                heapfile exception
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws Exception                  other exception
     */
    public boolean updateMap(RID rid, Map newMap)
            throws InvalidSlotNumberException,
            InvalidUpdateException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception {
        boolean status;
        HFPage dirPage = new HFPage();
        PageId currentDirPageId = new PageId();
        HFPage dataPage = new HFPage();
        PageId currentDataPageId = new PageId();
        RID currentDataPageRid = new RID();

        status = _findDataPage(rid,
                currentDirPageId, dirPage,
                currentDataPageId, dataPage,
                currentDataPageRid);

        if (status != true) return status;    // record not found
        Map aMap = new Map();
        aMap = dataPage.returnMap(rid);

        // Assume update a record with a record whose length is equal to
        // the original record

        if (newMap.getLength() != aMap.getLength()) {
            unpinPage(currentDataPageId, false /*undirty*/);
            unpinPage(currentDirPageId, false /*undirty*/);

            throw new InvalidUpdateException(null, "invalid record update");

        }

        // new copy of this record fits in old space;
        aMap.mapCopy(newMap);
        unpinPage(currentDataPageId, true /* = DIRTY */);

        unpinPage(currentDirPageId, false /*undirty*/);


        return true;
    }


    /**
     * Read record from file, returning pointer and length.
     *
     * @param rid Map ID
     * @return a Tuple. if Tuple==null, no more tuple
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws SpaceNotAvailableException no space left
     * @throws HFException                heapfile exception
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws Exception                  other exception
     */
    public Map getMap(RID rid)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception {
        boolean status;
        HFPage dirPage = new HFPage();
        PageId currentDirPageId = new PageId();
        HFPage dataPage = new HFPage();
        PageId currentDataPageId = new PageId();
        RID currentDataPageRid = new RID();

        status = _findDataPage(rid,
                currentDirPageId, dirPage,
                currentDataPageId, dataPage,
                currentDataPageRid);

        if (status != true) return null; // record not found

        Map aMap = new Map();
        aMap = dataPage.getMap(rid);

        /*
         * getMap has copied the contents of rid into mapPtr and fixed up
         * recLen also.  We simply have to unpin dirpage and datapage which
         * were originally pinned by _findDataPage.
         */

        unpinPage(currentDataPageId, false /*undirty*/);

        unpinPage(currentDirPageId, false /*undirty*/);


        return aMap;  //(true?)OK, but the caller need check if aMap==NULL

    }


    /**
     * Initiate a sequential scan.
     *
     * @throws InvalidTupleSizeException Invalid tuple size
     * @throws IOException               I/O errors
     */
//    public Scan openScan()
//            throws InvalidTupleSizeException,
//            IOException {
//        Scan newscan = new Scan(this);
//        return newscan;
//    }


    /**
     * Delete the file from the database.
     *
     * @throws InvalidSlotNumberException  invalid slot number
     * @throws InvalidTupleSizeException   invalid tuple size
     * @throws FileAlreadyDeletedException file is deleted already
     * @throws HFBufMgrException           exception thrown from bufmgr layer
     * @throws HFDiskMgrException          exception thrown from diskmgr layer
     * @throws IOException                 I/O errors
     */
    public void deleteBigt()
            throws InvalidSlotNumberException,
            FileAlreadyDeletedException,
            InvalidTupleSizeException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {
        if (_file_deleted)
            throw new FileAlreadyDeletedException(null, "file alread deleted");


        // Mark the deleted flag (even if it doesn't get all the way done).
        _file_deleted = true;

        // Deallocate all data pages
        PageId currentDirPageId = new PageId();
        currentDirPageId.pid = _firstDirPageId.pid;
        PageId nextDirPageId = new PageId();
        nextDirPageId.pid = 0;
        Page pageinbuffer = new Page();
        HFPage currentDirPage = new HFPage();
        Map aMap;

        pinPage(currentDirPageId, currentDirPage, false);
        //currentDirPage.openHFpage(pageinbuffer);

        RID rid = new RID();
        while (currentDirPageId.pid != INVALID_PAGE) {
            for (rid = currentDirPage.firstMap();
                 rid != null;
                 rid = currentDirPage.nextMap(rid)) {
                aMap = currentDirPage.getMap(rid);
                DataPageInfo dpinfo = new DataPageInfo(aMap);
                //int dpinfoLen = arecord.length;

                freePage(dpinfo.pageId);

            }
            // ASSERTIONS:
            // - we have freePage()'d all data pages referenced by
            // the current directory page.

            nextDirPageId = currentDirPage.getNextPage();
            freePage(currentDirPageId);

            currentDirPageId.pid = nextDirPageId.pid;
            if (nextDirPageId.pid != INVALID_PAGE) {
                pinPage(currentDirPageId, currentDirPage, false);
                //currentDirPage.openHFpage(pageinbuffer);
            }
        }

        delete_file_entry(_fileName);
    }

    /**
     * short cut to access the pinPage function in bufmgr package.
     *
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     *
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
        }

    } // end of unpinPage

    private void freePage(PageId pageno)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: freePage() failed");
        }

    } // end of freePage

    int count = 0;
    private PageId newPage(Page page, int num)
            throws HFBufMgrException {
        ++count;
        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseBM.newPage(page, num);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: newPage() failed");
        }

        return tmpId;

    } // end of newPage

    public int getCount(){
        return count;
    }
    private PageId get_file_entry(String filename)
            throws HFDiskMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: get_file_entry() failed");
        }

        return tmpId;

    } // end of get_file_entry

    private void add_file_entry(String filename, PageId pageno)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.add_file_entry(filename, pageno);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: add_file_entry() failed");
        }

    } // end of add_file_entry

    private void delete_file_entry(String filename)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: delete_file_entry() failed");
        }

    } // end of delete_file_entry

    public String getName(){
        return this.name;
    }

    public int getType(){
        return this.type;
    }

}// End of HeapFile
