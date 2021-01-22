package cn.ctyun.oos.server.processor.image.util;

import java.awt.Point;

import net.coobird.thumbnailator.geometry.Position;
import net.coobird.thumbnailator.geometry.Positions;

/**
 * 计算偏移的工具类
 * @author zhaowentao
 *
 */
public class PositionHelper {
    private static PositionHelper instance = new PositionHelper();
    
    private PositionHelper(){
        
    }
    
    public static PositionHelper getInstance(){
        return instance;
    }
    public static Position getPosition(int pos) {
        // TODO Auto-generated method stub
        Position position = null;
        switch(pos){
        case 1:
            position = Positions.TOP_LEFT;
            break;
        case 2:
            position = Positions.TOP_CENTER;
            break;
        case 3:
            position = Positions.TOP_RIGHT;
            break;
        case 4:
            position = Positions.CENTER_LEFT;
            break;
        case 5:
            position = Positions.CENTER;
            break;
        case 6:
            position = Positions.CENTER_RIGHT;
            break;
        case 7:
            position = Positions.BOTTOM_LEFT;
            break;
        case 8:
            position = Positions.BOTTOM_CENTER;
            break;
        case 9:
            position = Positions.BOTTOM_RIGHT;
            break;
        }
        
        return position;
    }
    public Point calculateVoffset(Position position, Point p, int voffset) {
        // TODO Auto-generated method stub
        Point point = new Point();
        point.x = p.x;
        point.y = p.y;
        if(position.equals( Positions.CENTER_LEFT)){
            point.y = p.y+voffset;
        }else if(position.equals( Positions.CENTER)){
            point.y = p.y+voffset;
        }else if(position.equals( Positions.CENTER_RIGHT)){
            point.y = p.y+voffset;
        }
        return point;
    }
    public  Padding calculate(Position  position,int x,int y){
        Padding p = new Padding();
        if(x==0 && y==0){
            return p;
        }
        
        if(position.equals( Positions.TOP_LEFT)){
            p.insertLeft = x;
            p.insertTop = y;
        }else if(position.equals( Positions.TOP_CENTER)){
            p.insertLeft = 0;
            p.insertTop = y;
        }else if(position.equals( Positions.TOP_RIGHT)){
            p.insertRight = x;
            p.insertTop = y;
        }else if(position.equals( Positions.CENTER_LEFT)){
            p.insertLeft = x;
            p.insertTop = 0;
        }else if(position.equals( Positions.CENTER)){
            p.insertLeft = 0;
            p.insertTop = 0;
        }else if(position.equals( Positions.CENTER_RIGHT)){
            p.insertRight = x;
            p.insertTop = 0;
        }else if(position.equals( Positions.BOTTOM_LEFT)){
            p.insertLeft = x;
            p.insertBottom = y;
        }else if(position.equals( Positions.BOTTOM_CENTER)){
            p.insertLeft = 0;
            p.insertBottom = y;
        }else if(position.equals( Positions.BOTTOM_RIGHT)){
            p.insertRight = x;
            p.insertBottom = y;
        }   
        return p;
    }
    
    
    public class Padding{
        //开始位置水平填充
        private int insertLeft;
        //开始位置垂直填充
        private int insertTop;
        //结束位置水平填充
        private int insertRight;
        //结束位置垂直填充
        private int insertBottom;
        public int getInsertLeft() {
            return insertLeft;
        }
        public void setInsertLeft(int insertLeft) {
            this.insertLeft = insertLeft;
        }
        public int getInsertTop() {
            return insertTop;
        }
        public void setInsertTop(int insertTop) {
            this.insertTop = insertTop;
        }
        public int getInsertRight() {
            return insertRight;
        }
        public void setInsertRight(int insertRight) {
            this.insertRight = insertRight;
        }
        public int getInsertBottom() {
            return insertBottom;
        }
        public void setInsertBottom(int insertBottom) {
            this.insertBottom = insertBottom;
        }
        
        
        
        
    }
    /**
     * 根据字体大小，计算阴影的偏移量
     * @param fontsize
     * @return
     */
    public int getShadowTranslation(int fontsize) {
        if(fontsize < 34) {
            return 1;
        } 
        if(fontsize < 140) {
            return 2;
        }
        
        return 3;
    }

   
}
