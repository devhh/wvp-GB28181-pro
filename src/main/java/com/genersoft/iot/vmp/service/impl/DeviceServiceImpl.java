//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.genersoft.iot.vmp.service.impl;

import com.genersoft.iot.vmp.conf.DynamicTask;
import com.genersoft.iot.vmp.gb28181.bean.Device;
import com.genersoft.iot.vmp.gb28181.bean.DeviceChannel;
import com.genersoft.iot.vmp.gb28181.bean.SsrcTransaction;
import com.genersoft.iot.vmp.gb28181.bean.SyncStatus;
import com.genersoft.iot.vmp.gb28181.event.SipSubscribe;
import com.genersoft.iot.vmp.gb28181.session.VideoStreamSessionManager;
import com.genersoft.iot.vmp.gb28181.task.ISubscribeTask;
import com.genersoft.iot.vmp.gb28181.task.impl.CatalogSubscribeTask;
import com.genersoft.iot.vmp.gb28181.task.impl.MobilePositionSubscribeTask;
import com.genersoft.iot.vmp.gb28181.transmit.cmd.ISIPCommander;
import com.genersoft.iot.vmp.gb28181.transmit.event.request.impl.message.response.cmd.CatalogResponseMessageHandler;
import com.genersoft.iot.vmp.service.IDeviceChannelService;
import com.genersoft.iot.vmp.service.IDeviceService;
import com.genersoft.iot.vmp.service.IMediaServerService;
import com.genersoft.iot.vmp.storager.IRedisCatchStorage;
import com.genersoft.iot.vmp.storager.IVideoManagerStorage;
import com.genersoft.iot.vmp.storager.dao.DeviceChannelMapper;
import com.genersoft.iot.vmp.storager.dao.DeviceMapper;
import com.genersoft.iot.vmp.utils.DateUtil;
import com.genersoft.iot.vmp.vmanager.bean.BaseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class DeviceServiceImpl implements IDeviceService {
    private static final Logger logger = LoggerFactory.getLogger(DeviceServiceImpl.class);
    private final String registerExpireTaskKeyPrefix = "device-register-expire-";
    @Autowired
    private DynamicTask dynamicTask;
    @Autowired
    private ISIPCommander sipCommander;
    @Autowired
    private CatalogResponseMessageHandler catalogResponseMessageHandler;
    @Autowired
    private IRedisCatchStorage redisCatchStorage;
    @Autowired
    private DeviceMapper deviceMapper;
    @Autowired
    private IDeviceChannelService deviceChannelService;
    @Autowired
    private DeviceChannelMapper deviceChannelMapper;
    @Autowired
    private IVideoManagerStorage storage;
    @Autowired
    private ISIPCommander commander;
    @Autowired
    private VideoStreamSessionManager streamSession;
    @Autowired
    private IMediaServerService mediaServerService;

    public DeviceServiceImpl() {
    }

    @Override
    public void online(Device device) {
        logger.info("[设备上线] deviceId：{}->{}:{}", new Object[]{device.getDeviceId(), device.getIp(), device.getPort()});
        Device deviceInRedis = this.redisCatchStorage.getDevice(device.getDeviceId());
        Device deviceInDb = this.deviceMapper.getDeviceByDeviceId(device.getDeviceId());
        String now = DateUtil.getNow();
        if (deviceInRedis != null && deviceInDb == null) {
            this.redisCatchStorage.clearCatchByDeviceId(device.getDeviceId());
        }

        device.setUpdateTime(now);
        if (device.getCreateTime() == null) {
            device.setOnline(1);
            device.setCreateTime(now);
            logger.info("[设备上线,首次注册]: {}，查询设备信息以及通道信息", device.getDeviceId());
            this.deviceMapper.add(device);
            this.redisCatchStorage.updateDevice(device);
            this.commander.deviceInfoQuery(device);
            this.sync(device);
        } else if (device.getOnline() == 0) {
            device.setOnline(1);
            device.setCreateTime(now);
            logger.info("[设备上线,离线状态下重新注册]: {}，查询设备信息以及通道信息", device.getDeviceId());
            this.deviceMapper.update(device);
            this.redisCatchStorage.updateDevice(device);
//            this.commander.deviceInfoQuery(device);
//            this.sync(device);
        } else {
            this.deviceMapper.update(device);
            this.redisCatchStorage.updateDevice(device);
        }

        if (device.getSubscribeCycleForCatalog() > 0) {
            this.addCatalogSubscribe(device);
        }

        if (device.getSubscribeCycleForMobilePosition() > 0) {
            this.addMobilePositionSubscribe(device);
        }

        String registerExpireTaskKey = "device-register-expire-" + device.getDeviceId();
        this.dynamicTask.startDelay(registerExpireTaskKey, () -> {
            this.offline(device.getDeviceId());
        }, device.getExpires() * 1000);
    }

    @Override
    public void offline(String deviceId) {
        Device device = this.deviceMapper.getDeviceByDeviceId(deviceId);
        if (device != null) {
            String registerExpireTaskKey = "device-register-expire-" + deviceId;
            this.dynamicTask.stop(registerExpireTaskKey);
            device.setOnline(0);
            this.redisCatchStorage.updateDevice(device);
            this.deviceMapper.update(device);
//            this.deviceChannelMapper.offlineByDeviceId(deviceId);
            List<SsrcTransaction> ssrcTransactions = this.streamSession.getSsrcTransactionForAll(deviceId, (String)null, (String)null, (String)null);
            if (ssrcTransactions != null && ssrcTransactions.size() > 0) {
                Iterator var5 = ssrcTransactions.iterator();

                while(var5.hasNext()) {
                    SsrcTransaction ssrcTransaction = (SsrcTransaction)var5.next();
                    this.mediaServerService.releaseSsrc(ssrcTransaction.getMediaServerId(), ssrcTransaction.getSsrc());
                    this.mediaServerService.closeRTPServer(deviceId, ssrcTransaction.getChannelId(), ssrcTransaction.getStream());
                    this.streamSession.remove(deviceId, ssrcTransaction.getChannelId(), ssrcTransaction.getStream());
                }
            }

            this.removeCatalogSubscribe(device);
            this.removeMobilePositionSubscribe(device);
        }
    }

    @Override
    public boolean addCatalogSubscribe(Device device) {
        if (device != null && device.getSubscribeCycleForCatalog() >= 0) {
            logger.info("[添加目录订阅] 设备{}", device.getDeviceId());
            CatalogSubscribeTask catalogSubscribeTask = new CatalogSubscribeTask(device, this.sipCommander, this.dynamicTask);
            int subscribeCycleForCatalog = Math.max(device.getSubscribeCycleForCatalog(), 30);
            this.dynamicTask.startCron(device.getDeviceId() + "catalog", catalogSubscribeTask, (subscribeCycleForCatalog - 1) * 1000);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean removeCatalogSubscribe(Device device) {
        if (device != null && device.getSubscribeCycleForCatalog() >= 0) {
            logger.info("[移除目录订阅]: {}", device.getDeviceId());
            String taskKey = device.getDeviceId() + "catalog";
            if (device.getOnline() == 1) {
                Runnable runnable = this.dynamicTask.get(taskKey);
                if (runnable instanceof ISubscribeTask) {
                    ISubscribeTask subscribeTask = (ISubscribeTask)runnable;
                    subscribeTask.stop();
                }
            }

            this.dynamicTask.stop(taskKey);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean addMobilePositionSubscribe(Device device) {
        if (device != null && device.getSubscribeCycleForMobilePosition() >= 0) {
            logger.info("[添加移动位置订阅] 设备{}", device.getDeviceId());
            MobilePositionSubscribeTask mobilePositionSubscribeTask = new MobilePositionSubscribeTask(device, this.sipCommander, this.dynamicTask);
            int subscribeCycleForCatalog = Math.max(device.getSubscribeCycleForMobilePosition(), 30);
            this.dynamicTask.startCron(device.getDeviceId() + "mobile_position", mobilePositionSubscribeTask, subscribeCycleForCatalog * 1000);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean removeMobilePositionSubscribe(Device device) {
        if (device != null && device.getSubscribeCycleForCatalog() >= 0) {
            logger.info("[移除移动位置订阅]: {}", device.getDeviceId());
            String taskKey = device.getDeviceId() + "mobile_position";
            if (device.getOnline() == 1) {
                Runnable runnable = this.dynamicTask.get(taskKey);
                if (runnable instanceof ISubscribeTask) {
                    ISubscribeTask subscribeTask = (ISubscribeTask)runnable;
                    subscribeTask.stop();
                }
            }

            this.dynamicTask.stop(taskKey);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public SyncStatus getChannelSyncStatus(String deviceId) {
        return this.catalogResponseMessageHandler.getChannelSyncProgress(deviceId);
    }

    @Override
    public Boolean isSyncRunning(String deviceId) {
        return this.catalogResponseMessageHandler.isSyncRunning(deviceId);
    }

    @Override
    public void sync(Device device) {
        if (this.catalogResponseMessageHandler.isSyncRunning(device.getDeviceId())) {
            logger.info("开启同步时发现同步已经存在");
        } else {
            int sn = (int)((Math.random() * 9.0 + 1.0) * 100000.0);
            this.catalogResponseMessageHandler.setChannelSyncReady(device, sn);
            this.sipCommander.catalogQuery(device, sn, (event) -> {
                String errorMsg = String.format("同步通道失败，错误码： %s, %s", event.statusCode, event.msg);
                this.catalogResponseMessageHandler.setChannelSyncEnd(device.getDeviceId(), errorMsg);
            });
        }
    }

    @Override
    public Device queryDevice(String deviceId) {
        return this.deviceMapper.getDeviceByDeviceId(deviceId);
    }

    @Override
    public List<Device> getAllOnlineDevice() {
        return this.deviceMapper.getOnlineDevices();
    }

    @Override
    public boolean expire(Device device) {
        Instant registerTimeDate = Instant.from(DateUtil.formatter.parse(device.getRegisterTime()));
        Instant expireInstant = registerTimeDate.plusMillis(TimeUnit.SECONDS.toMillis((long)device.getExpires()));
        return expireInstant.isBefore(Instant.now());
    }

    @Override
    public void checkDeviceStatus(Device device) {
        if (device != null && device.getOnline() != 0) {
            this.sipCommander.deviceStatusQuery(device, (SipSubscribe.Event)null);
        }
    }

    @Override
    public Device getDeviceByHostAndPort(String host, int port) {
        return this.deviceMapper.getDeviceByHostAndPort(host, port);
    }

    @Override
    public void updateDevice(Device device) {
        Device deviceInStore = this.deviceMapper.getDeviceByDeviceId(device.getDeviceId());
        if (deviceInStore == null) {
            logger.warn("更新设备时未找到设备信息");
        } else {
            if (!StringUtils.isEmpty(device.getName())) {
                deviceInStore.setName(device.getName());
            }

            if (!StringUtils.isEmpty(device.getCharset())) {
                deviceInStore.setCharset(device.getCharset());
            }

            if (!StringUtils.isEmpty(device.getMediaServerId())) {
                deviceInStore.setMediaServerId(device.getMediaServerId());
            }

            if (device.getSubscribeCycleForCatalog() > 0) {
                if (deviceInStore.getSubscribeCycleForCatalog() == 0 || deviceInStore.getSubscribeCycleForCatalog() != device.getSubscribeCycleForCatalog()) {
                    deviceInStore.setSubscribeCycleForCatalog(device.getSubscribeCycleForCatalog());
                    this.addCatalogSubscribe(deviceInStore);
                }
            } else if (device.getSubscribeCycleForCatalog() == 0 && deviceInStore.getSubscribeCycleForCatalog() != 0) {
                deviceInStore.setSubscribeCycleForCatalog(device.getSubscribeCycleForCatalog());
                this.removeCatalogSubscribe(deviceInStore);
            }

            if (device.getSubscribeCycleForMobilePosition() > 0) {
                if (deviceInStore.getSubscribeCycleForMobilePosition() == 0 || deviceInStore.getSubscribeCycleForMobilePosition() != device.getSubscribeCycleForMobilePosition()) {
                    deviceInStore.setMobilePositionSubmissionInterval(device.getMobilePositionSubmissionInterval());
                    deviceInStore.setSubscribeCycleForMobilePosition(device.getSubscribeCycleForMobilePosition());
                    this.addMobilePositionSubscribe(deviceInStore);
                }
            } else if (device.getSubscribeCycleForMobilePosition() == 0 && deviceInStore.getSubscribeCycleForMobilePosition() != 0) {
                this.removeMobilePositionSubscribe(deviceInStore);
            }

            if (!deviceInStore.getGeoCoordSys().equals(device.getGeoCoordSys())) {
                this.updateDeviceChannelGeoCoordSys(device);
            }

            String now = DateUtil.getNow();
            device.setUpdateTime(now);
            device.setCharset(device.getCharset().toUpperCase());
            device.setUpdateTime(DateUtil.getNow());
            if (this.deviceMapper.update(device) > 0) {
                this.redisCatchStorage.updateDevice(device);
            }

        }
    }

    private void updateDeviceChannelGeoCoordSys(Device device) {
        List<DeviceChannel> deviceChannels = this.deviceChannelMapper.getAllChannelWithCoordinate(device.getDeviceId());
        if (deviceChannels.size() > 0) {
            List<DeviceChannel> deviceChannelsForStore = new ArrayList();
            Iterator var4 = deviceChannels.iterator();

            while(var4.hasNext()) {
                DeviceChannel deviceChannel = (DeviceChannel)var4.next();
                deviceChannelsForStore.add(this.deviceChannelService.updateGps(deviceChannel, device));
            }

            this.deviceChannelService.updateChannels(device.getDeviceId(), deviceChannelsForStore);
        }

    }

    @Override
    public List<BaseTree<DeviceChannel>> queryVideoDeviceTree(String deviceId, String parentId, boolean onlyCatalog) {
        Device device = this.deviceMapper.getDeviceByDeviceId(deviceId);
        if (device == null) {
            return null;
        } else {
            List channelsForCivilCode;
            if (parentId != null && !parentId.equals(deviceId)) {
                List trees;
                if ("CivilCode".equals(device.getTreeType())) {
                    if (parentId.length() % 2 != 0) {
                        return null;
                    } else if (parentId.length() > 10) {
                        return null;
                    } else if (parentId.length() == 10) {
                        if (onlyCatalog) {
                            return null;
                        } else {
                            channelsForCivilCode = this.deviceChannelMapper.getChannelsByCivilCode(deviceId, parentId);
                            trees = this.transportChannelsToTree(channelsForCivilCode, parentId);
                            return trees;
                        }
                    } else {
                        channelsForCivilCode = this.deviceChannelMapper.getChannelsWithCivilCodeAndLength(deviceId, parentId, parentId.length() + 2);
                        if (!onlyCatalog) {
                            trees = this.deviceChannelMapper.getChannelsByCivilCode(deviceId, parentId);
                            channelsForCivilCode.addAll(trees);
                        }

                        trees = this.transportChannelsToTree(channelsForCivilCode, parentId);
                        return trees;
                    }
                } else if ("BusinessGroup".equals(device.getTreeType())) {
                    if (parentId.length() < 14) {
                        return null;
                    } else {
                        channelsForCivilCode = this.deviceChannelMapper.queryChannels(deviceId, parentId, (String)null, (Boolean)null, (Boolean)null,null);
                        trees = this.transportChannelsToTree(channelsForCivilCode, parentId);
                        return trees;
                    }
                } else {
                    return null;
                }
            } else {
                channelsForCivilCode = this.getRootNodes(deviceId, "CivilCode".equals(device.getTreeType()), true, !onlyCatalog);
                return this.transportChannelsToTree(channelsForCivilCode, "");
            }
        }
    }

    @Override
    public List<DeviceChannel> queryVideoDeviceInTreeNode(String deviceId, String parentId) {
        Device device = this.deviceMapper.getDeviceByDeviceId(deviceId);
        if (device == null) {
            return null;
        } else {
            List channels;
            if (parentId != null && !parentId.equals(deviceId)) {
                if ("CivilCode".equals(device.getTreeType())) {
                    if (parentId.length() % 2 != 0) {
                        return null;
                    } else if (parentId.length() > 10) {
                        return null;
                    } else if (parentId.length() == 10) {
                        channels = this.deviceChannelMapper.getChannelsByCivilCode(deviceId, parentId);
                        return channels;
                    } else {
                        channels = this.deviceChannelMapper.getChannelsByCivilCode(deviceId, parentId);
                        return channels;
                    }
                } else if ("BusinessGroup".equals(device.getTreeType())) {
                    if (parentId.length() < 14) {
                        return null;
                    } else {
                        channels = this.deviceChannelMapper.queryChannels(deviceId, parentId, (String)null, (Boolean)null, (Boolean)null,null);
                        return channels;
                    }
                } else {
                    return null;
                }
            } else {
                channels = this.getRootNodes(deviceId, "CivilCode".equals(device.getTreeType()), false, true);
                return channels;
            }
        }
    }

    private List<BaseTree<DeviceChannel>> transportChannelsToTree(List<DeviceChannel> channels, String parentId) {
        if (channels == null) {
            return null;
        } else {
            List<BaseTree<DeviceChannel>> treeNotes = new ArrayList();
            if (channels.size() == 0) {
                return treeNotes;
            } else {
                BaseTree node;
                for(Iterator var4 = channels.iterator(); var4.hasNext(); treeNotes.add(node)) {
                    DeviceChannel channel = (DeviceChannel)var4.next();
                    node = new BaseTree();
                    node.setId(channel.getChannelId());
                    node.setDeviceId(channel.getDeviceId());
                    node.setName(channel.getName());
                    node.setPid(parentId);
                    node.setBasicData(channel);
                    node.setParent(false);
                    if (channel.getChannelId().length() <= 8) {
                        node.setParent(true);
                    } else {
                        String gbCodeType = channel.getChannelId().substring(10, 13);
                        node.setParent(gbCodeType.equals("215") || gbCodeType.equals("216"));
                    }
                }

                Collections.sort(treeNotes);
                return treeNotes;
            }
        }
    }

    private List<DeviceChannel> getRootNodes(String deviceId, boolean isCivilCode, boolean haveCatalog, boolean haveChannel) {
        if (!haveCatalog && !haveChannel) {
            return null;
        } else {
            List<DeviceChannel> result = new ArrayList();
            if (isCivilCode) {
                Integer length = this.deviceChannelMapper.getChannelMinLength(deviceId);
                if (length == null) {
                    return null;
                }

                List nonstandardNode;
                if (length <= 10) {
                    if (haveCatalog) {
                        nonstandardNode = this.deviceChannelMapper.getChannelsWithCivilCodeAndLength(deviceId, (String)null, length);
                        if (nonstandardNode != null && nonstandardNode.size() > 0) {
                            result.addAll(nonstandardNode);
                        }
                    }

                    if (haveChannel) {
                        nonstandardNode = this.deviceChannelMapper.getChannelWithoutCiviCode(deviceId);
                        if (nonstandardNode != null && nonstandardNode.size() > 0) {
                            result.addAll(nonstandardNode);
                        }
                    }
                } else if (haveChannel) {
                    nonstandardNode = this.deviceChannelMapper.queryChannels(deviceId, (String)null, (String)null, (Boolean)null, (Boolean)null,null);
                    if (nonstandardNode != null && nonstandardNode.size() > 0) {
                        result.addAll(nonstandardNode);
                    }
                }
            } else {
                List<DeviceChannel> deviceChannels = this.deviceChannelMapper.getBusinessGroups(deviceId, "215");
                if (deviceChannels != null && deviceChannels.size() > 0) {
                    result.addAll(deviceChannels);
                }
            }

            return result;
        }
    }
}
