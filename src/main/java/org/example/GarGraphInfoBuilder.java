/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import com.alibaba.graphar.graphinfo.EdgeInfo;
import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.graphinfo.Property;
import com.alibaba.graphar.graphinfo.PropertyGroup;
import com.alibaba.graphar.graphinfo.VertexInfo;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.stdcxx.StdVector;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.types.DataType;
import com.alibaba.graphar.types.FileType;
import com.alibaba.graphar.types.Type;
import com.alibaba.graphar.util.InfoVersion;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;

import java.util.List;

public class GarGraphInfoBuilder {

    public static final int DEFAULT_CHUNK_SIZE = 2;
    private static final StdVector.Factory<Property> propertyVecFactory =
            StdVector.getStdVectorFactory("std::vector<GraphArchive::Property>");

    public static GraphInfo buildGarGraphInfo(String graphName, List<EdgeInfo> edgeInfos, List<VertexInfo> vertexInfos) {
        GraphInfo graphInfo = GraphInfo.factory.create(StdString.create(graphName), InfoVersion.create(1));
        for (EdgeInfo edgeInfo : edgeInfos) {
            graphInfo.addEdge(edgeInfo);
        }
        for (VertexInfo vertexInfo : vertexInfos) {
            graphInfo.addVertex(vertexInfo);
        }
        return graphInfo;
    }

    public static EdgeInfo buildGarEdgeInfo(SchemaManager schema, EdgeLabel edgeLabel) {
        StdString srcLabelName = StdString.create(edgeLabel.sourceLabel());
        StdString edgeLabelName = StdString.create(edgeLabel.name());
        StdString dstLabelName = StdString.create(edgeLabel.targetLabel());
        EdgeInfo edgeInfo = EdgeInfo.factory.create(srcLabelName, edgeLabelName, dstLabelName, DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE, false, InfoVersion.create(1));
        edgeInfo.addAdjList(AdjListType.unordered_by_source, FileType.CSV);
        for (String propertyName : edgeLabel.properties()) {
            PropertyKey propertyKey = schema.getPropertyKey(propertyName);
            edgeInfo.addPropertyGroup(garProperty2GarPropertyGroup(hugePropertyKey2GarProperty(propertyKey)), AdjListType.unordered_by_source);
//            System.out.println(edgeInfo.getPropertyGroups(AdjListType.unordered_by_dest).value().data());
        }
        return edgeInfo;
    }

    public static VertexInfo buildGarVertexInfo(SchemaManager schema, VertexLabel vertexLabel) {
        VertexInfo vertexInfo = VertexInfo.factory.create(StdString.create(vertexLabel.name()), DEFAULT_CHUNK_SIZE, InfoVersion.create(1));
        for (String propertyName : vertexLabel.properties()) {
            PropertyKey propertyKey = schema.getPropertyKey(propertyName);
            vertexInfo.addPropertyGroup(garProperty2GarPropertyGroup(hugePropertyKey2GarProperty(propertyKey)));
        }
        return vertexInfo;
    }

    private static PropertyGroup garProperty2GarPropertyGroup(Property property) {
        StdVector<Property> propertyStdVector = propertyVecFactory.create();
        propertyStdVector.push_back(property);
        return PropertyGroup.factory.create(propertyStdVector, FileType.CSV);
    }

    private static Property hugePropertyKey2GarProperty(PropertyKey propertyKey) {
        Property property = Property.factory.create();
        property.setName(StdString.create(propertyKey.name()));
        property.setType(DataType.factory.create(hugeDataType2GarType(propertyKey.dataType())));
        property.setPrimary(false);
        return property;
    }

    private static com.alibaba.graphar.types.Type hugeDataType2GarType(org.apache.hugegraph.structure.constant.DataType hugeDataType) {
        switch (hugeDataType) {
            case BOOLEAN:
                return com.alibaba.graphar.types.Type.BOOL;
            case INT:
                return com.alibaba.graphar.types.Type.INT32;
            case LONG:
                return com.alibaba.graphar.types.Type.INT64;
            case FLOAT:
                return com.alibaba.graphar.types.Type.FLOAT;
            case DOUBLE:
                return com.alibaba.graphar.types.Type.DOUBLE;
            case TEXT:
                return com.alibaba.graphar.types.Type.STRING;
            case DATE:
                return Type.USER_DEFINED;
            default:
                throw new IllegalArgumentException("Unknown data type: " + hugeDataType);
        }
    }

}
