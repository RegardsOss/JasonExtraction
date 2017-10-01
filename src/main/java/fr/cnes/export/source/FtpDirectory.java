/**
 * ****************************************************************************
 * Copyright 2017 CNES - CENTRE NATIONAL d'ETUDES SPATIALES
 *
 * This file is part of Regards.
 *
 * Regards is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Regards is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Regards.  If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************
 */
package fr.cnes.export.source;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.restlet.Client;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

/**
 *
 * @author Jean-Christophe Malapert <jean-christophe.malapert@cnes.fr>
 */
public class FtpDirectory implements Directory {

    private final ClientResource directory;
    private final FtpDirectoryParser ftp;
    private final String url;

    public FtpDirectory(String url) throws URISyntaxException, IOException, ResourceException {
        this.url = url;
        Context context = new Context();
        context.getParameters().add("readTimeout", "200000");
        Client client = new Client(context, Protocol.FTP);
        directory = new ClientResource(new URI(url));
        directory.setNext(client);
        directory.setRetryAttempts(20);
        directory.setRetryOnError(true);
        directory.setRetryDelay(10000);        
        Representation rep = directory.get();
        ftp = new FtpDirectoryParser(rep.getReader());
    }

    @Override
    public String getSourceDirectory() {
        return this.url;
    }

    @Override
    public String[] getNextRecord() {
        return ftp.getNextLine();
    }

    @Override
    public boolean isDirectory() {
        return ftp.isDirectory();
    }

    @Override
    public int getTotalRecords() {
        return ftp.getTotalLine();
    }

    @Override
    public void close() {
        ftp.close();
        directory.release();
    }

}
