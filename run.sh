
rm -rf data/tmp
echo "deleted tmp"

coffee -c *.coffee

coffee index.coffee import at
coffee index.coffee import cz
coffee index.coffee import de

coffee index.coffee export places
